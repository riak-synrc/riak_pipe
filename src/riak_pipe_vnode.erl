%% -------------------------------------------------------------------
%%
%% Copyright (c) 2011 Basho Technologies, Inc.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

%% @doc The vnode, where the queues live.

-module(riak_pipe_vnode).
-behaviour(riak_core_vnode).

-export([start_vnode/1,
         init/1,
         terminate/2,
         handle_command/3,
         is_empty/1,
         delete/1,
         handle_handoff_command/3,
         handoff_starting/2,
         handoff_cancelled/1,
         handoff_finished/2,
         handle_handoff_data/2,
         encode_handoff_item/2,
         handle_exit/3,
         handle_info/2,
         handle_coverage/4]).
-export([queue_work/2,
         queue_work/3,
         queue_work/4,
         queue_work/5,
         queue_work_list/3,
         queue_work_bins/3,
         eoi/2,
         next_input/2,
         next_input_list/2,
         reply_archive/3,
         status/1,
         status/2]).
-export([hash_for_partition/1]).

-include_lib("riak_core/include/riak_core_vnode.hrl"). %% ?FOLD_REQ
-include("riak_pipe.hrl").
-include("riak_pipe_log.hrl").
-include("riak_pipe_debug.hrl").

-export_type([chashfun/0,
              chash/0,
              partition/0, %% from riak_core_vnode.hrl
              nval/0,
              qtimeout/0,
              qerror/0,
              nitype/0]).
-type chashfun() :: {Module :: atom(), Function :: atom()}
                  | chash()
                  | follow
                  | sink
                  | fun((term()) -> chash()). % 1.0.x compatibility
-type chash() :: chash:index().
-type nval() :: pos_integer()
              | {Module :: atom(), Function :: atom()}
              | fun((term()) -> pos_integer()). % 1.0.x compatibility
-type qtimeout() :: noblock | infinity.
-type qerror() :: worker_limit_reached
                | worker_startup_failed
                | timeout
                | forwarding
                | preflist_exhausted.
-type down_message() :: {'DOWN', reference(), process, pid(), term()}.
-type nitype() :: one | list.

-define(DEFAULT_WORKER_LIMIT, 50).
-define(DEFAULT_WORKER_Q_LIMIT, 4096).
-define(FORWARD_WORKER_MODULE, riak_pipe_w_fwd).

-record(worker_perf, {started :: calendar:t_now(),
                       processed = 0 :: non_neg_integer(),
                       failures = 0 :: non_neg_integer(),
                       work_time = 0 :: non_neg_integer(),
                       idle_time = 0 :: non_neg_integer(),
                       last_time :: calendar:t_now()}).
-record(worker, {pid :: pid(),
                 fitting :: #fitting{},
                 details :: #fitting_details{},
                 state :: {working, term()} | {waiting, nitype()} | init,
                 inputs_done :: boolean(),
                 q :: queue(),
                 q_limit :: pos_integer(),
                 blocking :: queue(),
                 handoff :: undefined | {waiting, term()},
                 perf :: #worker_perf{}}).
-record(worker_handoff, {fitting :: #fitting{},
                         queue :: queue(),
                         blocking :: queue(),
                         archive :: term()}).
-record(handoff, {fold :: fun((Key::term(), Value::term(), Acc::term())
                              -> NewAcc::term()),
                  acc :: term(),
                  sender :: sender()}).

-record(state, {partition :: partition(),
                worker_sup :: pid(),
                workers :: [#worker{}],
                worker_limit :: pos_integer(),
                worker_q_limit :: pos_integer(),
                workers_archiving :: [#worker{}],
                handoff :: undefined | starting | cancelled | finished
                         | #handoff{}}).

-opaque state() :: #state{}.

-record(cmd_enqueue, {fitting :: #fitting{},
                      input :: term(),
                      timeout :: qtimeout(),
                      usedpreflist :: riak_core_apl:preflist()}).
%% enqlist is enqueue, but for the queue_list function, which needs a
%% different reply format to operate correctly
-record(cmd_enqlist, {fitting :: #fitting{},
                      inputs :: list(),
                      timeout :: qtimeout(),
                      usedpreflist :: riak_core_apl:preflist()}).
-record(cmd_eoi, {fitting :: #fitting{}}).
-record(cmd_next_input, {fitting :: #fitting{},
                         type :: nitype()}).
-record(cmd_archive, {fitting :: #fitting{},
                      archive :: term()}).
-record(cmd_status, {sender :: term(),
                     fittings :: all | [#fitting{}]}).
%% used during queue_list multiplexing
-record(bin, {
          inputs :: list(),
          hash :: chash(),
          vnode :: pid(),
          monitor :: reference(),
          used :: riak_core_apl:preflist()
         }).


%% API

%% @doc Start the vnode, if it isn't started already.
-spec start_vnode(partition()) -> {ok, pid()}.
start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

%% @doc Evaluate `Validator(Thing)'; return `Thing' if the evaluation
%%      returns `true', or call {@link erlang:exit/1} if the
%%      evaluation returns `false'. (`Msg' is used in the error reason
%%      passed to exit/1.)
-spec validate_or_exit(term(), fun((term) -> boolean()), string()) ->
         term().
validate_or_exit(Thing, Validator, Msg) ->
    case Validator(Thing) of
        true -> Thing;
        false ->
            lager:error(Msg++"~n   (found ~p)", [Thing]),
            exit({invalid_config, {Msg, Thing}})
    end.

%% @doc Initialize the vnode.  This function validates the limits set
%%      in the application environment, and starts the worker
%%      supervisor.
%%
%%      Two application environment variables matter to the vnode:
%%<dl><dt>
%%      `worker_limit'
%%</dt><dd>
%%      Positive integer, default 50. The maximum number of workers
%%      allowed to operate on this vnode.
%%</dd><dt>
%%      `worker_queue_limit'
%%</dt><dd>
%%      Positive integer, default 4096. The maximum length of each
%%      worker's input queue.  The actual cap for a fitting's queue is
%%      the lesser of this number and the `q_limit' specified in the
%%      startup spec.
%%</dd></dl>
-spec init([partition()]) -> {ok, state()}.
init([Partition]) ->
    WL = validate_or_exit(app_helper:get_env(riak_pipe, worker_limit,
                                             ?DEFAULT_WORKER_LIMIT),
                          fun(X) -> is_integer(X) andalso (X > 0) end,
                          "riak_pipe.worker_limit must be"
                          " an integer greater than zero"),
    WQL = validate_or_exit(app_helper:get_env(riak_pipe, worker_queue_limit,
                                              ?DEFAULT_WORKER_Q_LIMIT),
                           fun(X) -> is_integer(X) andalso (X > 0) end,
                           "riak_pipe.worker_queue_limit must be"
                           " an integer greater than zero"),
    {ok, WorkerSup} = riak_pipe_vnode_worker_sup:start_link(Partition, self()),
    {ok, #state{
       partition=Partition,
       worker_sup=WorkerSup,
       workers=[],
       worker_limit=WL,
       worker_q_limit=WQL,
       workers_archiving=[]
      }}.

%% @doc Get hash value in a range owned by any local vnode.  Used to
%%      semi-randomly choose a local vnode if the spec for the head
%%      fitting of a pipeline uses a consistent-hashing function of
%%      `follow'.
-spec any_local_vnode() -> chash().
any_local_vnode() ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    hash_for_partition(hd(riak_core_ring:my_indices(Ring))).

%% @doc Produce a hash value in the range handled by the given
%%      partition.  Used to support the `follow' chashfun.
-spec hash_for_partition(partition()) -> chash().
hash_for_partition(0) ->
    <<(trunc(math:pow(2,160))-1):160/integer>>;
hash_for_partition(I) ->
    %% partition indices indicate the *last* point
    %% in the hash space they own
    <<(I-1):160/integer>>.

%% @equiv queue_work(Fitting, Input, infinity)
queue_work(Fitting, Input) ->
    queue_work(Fitting, Input, infinity).

%% @equiv queue_work(Fitting, Input, Timeout, [])
queue_work(Fitting, Input, Timeout) ->
    queue_work(Fitting, Input, Timeout, []).

%% @doc Queue the given `Input' for processing by the `Fitting'.  This
%%      function handles getting the input to the correct vnode by
%%      evaluating the fitting's consistent-hashing function
%%      (`chashfun') on the input.
-spec queue_work(riak_pipe:fitting(), term(), qtimeout(),
                 riak_core_apl:preflist()) ->
         ok | {error, [qerror()]}.
queue_work(Fitting, Input, Timeout, UsedPreflist) ->
    Hash = work_hash(Fitting, Input),
    queue_work(Fitting, Input, Timeout, UsedPreflist, Hash).

%% @doc Compute the hash value for this fitting-input pair.
-spec work_hash(riak_pipe:fitting(), term()) -> chash().
work_hash(#fitting{chashfun=follow}, _Input) ->
    %% this should only happen if someone sets up a pipe with
    %% the first fitting as chashfun=follow
    any_local_vnode();
work_hash(#fitting{chashfun={Module, Function}}, Input) ->
    Module:Function(Input);
work_hash(#fitting{chashfun=Hash}, _Input) when not is_function(Hash) ->
    Hash;
work_hash(#fitting{chashfun=HashFun}, Input) ->
    %% 1.0.x compatibility
    riak_pipe_fun:compat_apply(HashFun, [Input]).

%% @doc Queue the given `Input' for processing the the `Fitting' on
%%      the vnode specified by `Hash'.  This version of the function
%%      is used to support the `follow' chashfun, by allowing a worker
%%      to send the input directly to the vnode it works for.
%%
%%      `Timeout' may be any of the following:
%%<dl><dt>
%%      `infinity'
%%</dt><dd>
%%      Never timeout.  Wait as long as necessary to get the input in
%%      the queue.
%%</dd><dt>
%%      `noblock'
%%</dt><dd>
%%      Timeout if the vnode cannot immediately queue the input.
%%      `noblock' will wait as long as necessary for a response from
%%      the vnode, but will direct the vnode not to block the request
%%      if the queue is full.
%%</dd></dl>
-spec queue_work(riak_pipe:fitting(), term(),
                 qtimeout(), riak_core_apl:preflist(), chash()) ->
         ok | {error, [qerror()]}.
queue_work(Fitting, Input, Timeout, UsedPreflist, Hash) ->
    queue_work_erracc(Fitting, Input, Timeout, UsedPreflist, Hash, []).

%% @doc Internal implementation of queue_work, to accumulate errors
%%      returned by each failed vnode enqueue for cumulative failure
%%      return.
-spec queue_work_erracc(riak_pipe:fitting(), term(),
                        qtimeout(), riak_core_apl:preflist(), chash(),
                        [qerror()]) ->
         ok | {error, [qerror()]}.
queue_work_erracc(#fitting{nval=NVal}=Fitting,
                  Input, Timeout, UsedPreflist, Hash, ErrAcc) ->
    case remaining_preflist(Input, Hash, NVal, UsedPreflist) of
        [NextPref|_] ->
            case queue_work_send(Fitting, Input, Timeout,
                                 [NextPref|UsedPreflist]) of
                ok -> ok;
                {error, Error} ->
                    queue_work_erracc(Fitting, Input, Timeout,
                                      [NextPref|UsedPreflist], Hash,
                                      [Error|ErrAcc])
            end;
        [] ->
            if ErrAcc == [] ->
                    %% may happen if a fitting worker asks to forward
                    %% the input, but there is no more preflist to
                    %% forward to
                    {error, [preflist_exhausted]};
               true ->
                    {error, ErrAcc}
            end
    end.

%% @doc Compute the elements of the preflist that have not been
%%      attempted for this input yet.
-spec remaining_preflist(term(), chash(), nval(),
                         riak_core_apl:preflist()) ->
         riak_core_apl:preflist().
remaining_preflist(Input, Hash, NVal, UsedPreflist) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    Nodes = riak_core_node_watcher:nodes(riak_pipe),
    IntNVal = if is_integer(NVal)  -> NVal;
                 is_tuple(NVal)    -> {Mod, Fun} = NVal, Mod:Fun(Input);
                 %% 1.0.x compatibility
                 is_function(NVal) ->
                      riak_pipe_fun:compat_apply(NVal, [Input])
              end,
    %% it's possible that node availability changes could cause
    %% different vnodes to be available at different evaluations, so
    %% we have to check the length of UsedPreflist explicitly, instead
    %% of just expecting to filter all of the elements it contains
    if length(UsedPreflist) < IntNVal ->
            Preflist = riak_core_apl:get_apl(Hash, IntNVal, Ring, Nodes),
            Preflist--UsedPreflist;
       true ->
            []
    end.

%% @doc Do the actual sending of the work to the vnode, as well as
%%      receiving the response.
-spec queue_work_send(riak_pipe:fitting(), term(), qtimeout(),
                      riak_core_apl:preflist()) ->
         ok | {error, term()}.
queue_work_send(#fitting{ref=Ref}=Fitting,
                Input, Timeout,
                [{Index,Node}|_]=UsedPreflist) ->
    try riak_core_vnode_master:command_return_vnode(
      {Index, Node},
      #cmd_enqueue{fitting=Fitting, input=Input, timeout=Timeout,
                   usedpreflist=UsedPreflist},
      {raw, Ref, self()},
      riak_pipe_vnode_master) of
        {ok, VnodePid} ->
            %% monitor in case the vnode is gone before it
            %% responds to this request
            MonRef = erlang:monitor(process, VnodePid),
            %% block until input confirmed queued, for backpressure
            receive
                {Ref, Reply} ->
                    erlang:demonitor(MonRef),
                    Reply;
                {'DOWN',MonRef,process,VnodePid,Reason} ->
                    {error, {vnode_down, Reason}}
            end
    catch exit:{{nodedown, Node}, _GenServerCall} ->
            %% node died between services check and gen_server:call
            {error, {nodedown, Node}}
    end.

%% @doc Queue all of the given `Inputs` for processing at their
%% correct vnodes. This is more efficient than calling queue_work/3 on
%% each element of the list, because it doesn't wait on each input's
%% ack until it's time to queue another input on that vnode (or when
%% all inputs have been queued).
-spec queue_work_list(riak_pipe:fitting(), list(), qtimeout()) ->
         {ok | {error, term()}, Remaining::list()}.
queue_work_list(Fitting, Inputs, Timeout) ->
    %% can't use the implementation until the whole cluster supports it
    case riak_core_capability:get({riak_pipe, queue_list}) of
        native -> queue_work_list_native(Fitting, Inputs, Timeout);
        emulate -> queue_work_list_emulate(Fitting, Inputs, Timeout)
    end.

%% @doc Fall back on doing one `queue_work' per input for clusters
%% that have a mix of post-1.2 and 1.2-or-earlier nodes.
%%
%% This is incredibly naive, but should be essentially what every
%% fitting would have done before `queue_work_list': iterate through
%% the list as long as each queue request is successful.
queue_work_list_emulate(Fitting, [Next|Inputs], Timeout) ->
    case queue_work(Fitting, Next, Timeout) of
        ok ->
            queue_work_list_emulate(Fitting, Inputs, Timeout);
        {error,_}=Error ->
            {Error, Inputs}
    end;
queue_work_list_emulate(_Fitting, [], _Timeout) ->
    {ok, []}.

%% @doc Actual implementation of post-1.2 `queue_work_list' (internal).
queue_work_list_native(Fitting, Inputs, Timeout) ->
    queue_work_bins(Fitting, bin_inputs(Fitting, Inputs), Timeout).

queue_work_bins(Fitting, IBins, Timeout) ->
    %% bootstrap the pump by sending the first enqueue requests
    FirstQFun = fun(B, {Bins, Unqueued, Result}) ->
                        case queue_work_bin(Fitting, B, Timeout, []) of
                            {ok, Bin} ->
                                {[Bin|Bins], Unqueued, Result};
                            {error, _}=Error ->
                                {Bins, [B|Unqueued], Error}
                        end
                end,
    {Bins, Unqueued, Result} = lists:foldl(FirstQFun, {[], [], ok}, IBins),

    %% keep the pump going until we've made it the whole way through
    collect_bins(Fitting, Bins, Timeout, Unqueued, [], Result).

%% @doc Partition Inputs into bins according to the head of their preflist.
-spec bin_inputs(riak_pipe:fitting(), [term()]) ->
         [ Bin::{chash(), [term()]} ].
bin_inputs(Fitting, Inputs) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    %% some fittings expect their outputs to remain in the order
    %% they're given; foldr (instead of foldl) allows the code for
    %% assembling each list to cons without having to reverse later
    PBins = lists:foldr(
              fun(I, Acc) ->
                      P = riak_core_ring:responsible_index(
                            work_hash(Fitting, I), Ring),
                      case lists:keytake(P, 1, Acc) of
                          {value, {_, RestI}, NewAcc} ->
                              [{P,[I|RestI]}|NewAcc];
                          false ->
                              [{P, [I]}|Acc]
                      end
              end,
              [],
              Inputs),
    [ {work_hash(Fitting, hd(Bin)), Bin} || {_P, Bin} <- PBins].

%% @doc The multiplexing pump. Waits for a reply from any vnode, then
%% sends the next input scheduled for that vnode.
-spec collect_bins(riak_pipe:fitting(), [#bin{}], qtimeout(),
                   [ [term()] ], [ down_message() ],
                   ok | {error, Reason}) ->
         {ok | {error, Reason}, Remaining::[term()]}.
collect_bins(_Fitting, [], _Timeout, Unqueued, UnknownDowns, Result) ->
    %% let whoever was waiting for these pick them up now
    [ self() ! Down || Down <- UnknownDowns],
    {Result, lists:append(Unqueued)};
collect_bins(Fitting, Bins, Timeout, Uq, Ud, Result) ->
    case receive_bin(Fitting, Bins) of
        {ok, #bin{}, Rest} ->
            %% inputs finished for this bin
            collect_bins(Fitting, Rest, Timeout, Uq, Ud, Result);
        {{error, E1}, #bin{inputs=Inputs, used=Used, hash=Hash}, Rest} ->
            {IsTimeout, Leftover} =
                case E1 of
                    {timeout, Accepted} ->
                        %% some of a multi-input
                        %% enqueue didn't fit
                        {true, lists:nthtail(Accepted, Inputs)};
                    timeout ->
                        %% a single-input enqueue didn't fit
                        {true, Inputs};
                    _ ->
                        %% some other error (like worker startup)
                        {false, Inputs}
                end,
            %% error; requeue elsewhere
            case queue_work_bin(Fitting, {Hash, Leftover}, Timeout, Used) of
                {ok, NewBin} ->
                    collect_bins(Fitting, [NewBin|Rest],
                                 Timeout, Uq, Ud, Result);
                {error, E2}=Error ->
                    if Timeout =:= noblock,
                       IsTimeout,
                       E2 =:= preflist_exhausted ->
                            %% not an error to run out of options
                            %% after timing out on a noblock
                            collect_bins(Fitting, Rest, Timeout,
                                         [Leftover|Uq], Ud, Result);
                       true ->
                            collect_bins(Fitting, Rest, Timeout,
                                         [Leftover|Uq], Ud, Error)
                    end
            end;
        {error, {unknown_down, Down}} ->
            collect_bins(Fitting, Bins, Timeout, Uq, [Down|Ud], Result)
    end.

%% @doc Wait for a response from (or death of) any vnode that we have
%% sent a queue request to.
-spec receive_bin(riak_pipe:fitting(), [#bin{}]) ->
         {ok | {error, term()}, #bin{}, [#bin{}]}
       | {error, {unknown_down, down_message()}}.
receive_bin(#fitting{ref=Ref}=Fitting, Bins) ->
    receive
        {Ref, {VnodePid, Reply}}=M ->
            case lists:keytake(VnodePid, #bin.vnode, Bins) of
                {value, #bin{monitor=MonRef}=Bin, Rest} ->
                    erlang:demonitor(MonRef, [flush]),
                    {Reply, Bin, Rest};
                _ ->
                    %% not meant for us
                    lager:debug(
                      "Unknown reply in pipe bin sender: ~p~n", [M]),
                    receive_bin(Fitting, Bins)
            end;
        {'DOWN', MonRef, process, _VnodePid, Reason}=Down ->
            case lists:keytake(MonRef, #bin.monitor, Bins) of
                {value, Bin, Rest} ->
                    {{error, {'DOWN', Reason}}, Bin, Rest};
                _ ->
                    %% not meant for us; abort
                    {error, {unknown_down, Down}}
            end
    end.

%% @doc Send a queue request to a vnode. Sets up a monitor on that
%% vnode if successful.
-spec queue_work_bin(riak_pipe:fitting(), [term()], qtimeout(),
                     riak_core_apl:preflist()) ->
        {ok, #bin{}} | {error, term()}.
queue_work_bin(#fitting{nval=Nval}=Fitting,
               {Hash, [Head|_]=Inputs}, Timeout, Used) ->
    %% TODO: this assumes all of Inputs have the same N-value
    Remaining = remaining_preflist(Head, Hash, Nval, Used),
    case queue_work_bin_send(Fitting, Inputs, Timeout, Used, Remaining) of
        {ok, VnodePid, NewUsed} ->
            MonRef = erlang:monitor(process, VnodePid),
            {ok, #bin{inputs=Inputs,
                      hash=Hash,
                      vnode=VnodePid,
                      monitor=MonRef,
                      used=NewUsed}};
        {error, _}=Error ->
            Error
    end.

%% @doc Send the queue request to the next vnode in the preflist.
-spec queue_work_bin_send(riak_pipe:fitting(), [term()], qtimeout(),
                          riak_core_apl:preflist(),
                          riak_core_apl:preflist()) ->
         {ok, pid(), riak_core_apl:preflist()}
       | {error, preflist_exhausted}.
queue_work_bin_send(_Fitting, _Inputs, _Timeout, _Used, []) ->
    {error, preflist_exhausted};
queue_work_bin_send(#fitting{ref=Ref}=Fitting,
                    Inputs, Timeout, Used, [Pref|Remaining]) ->
    NewUsed = [Pref|Used],
    Cmd = #cmd_enqlist{fitting=Fitting,
                       inputs=Inputs,
                       timeout=Timeout,
                       usedpreflist=NewUsed},
    Sender = {raw, Ref, self()},
    try riak_core_vnode_master:command_return_vnode(
          Pref, Cmd, Sender, riak_pipe_vnode_master) of
        {ok, VnodePid} ->
            {ok, VnodePid, NewUsed};
        {error, _} ->
            queue_work_bin_send(Fitting, Inputs, Timeout, NewUsed, Remaining)
    catch exit:{{nodedown, _Node}, _GenServerCall} ->
            %% node died between services check and gen_server:call
            queue_work_bin_send(Fitting, Inputs, Timeout, NewUsed, Remaining)
    end.

%% @doc Send end-of-inputs for a fitting to a vnode.  Note: this
%%      should only be called by `riak_pipe_fitting' processes.  This
%%      will cause the vnode to shutdown the worker, dispose of the
%%      queue, and send a `done' to the fitting, once the queue is
%%      emptied.
-spec eoi(pid(), riak_pipe:fitting()) -> ok.
eoi(Pid, Fitting) ->
    riak_core_vnode:send_command(Pid, #cmd_eoi{fitting=Fitting}).

%% @doc Request the next input from the queue for the given fitting
%%      from a vnode.  Note: this should only be called by the worker
%%      process for that fitting-vnode pair.  This will cause the
%%      vnode to send the next input to the worker process for this
%%      fitting.
-spec next_input(pid(), riak_pipe:fitting()) -> ok.
next_input(Pid, Fitting) ->
    riak_core_vnode:send_command(
      Pid, #cmd_next_input{fitting=Fitting, type=one}).

%% @doc Request all inputs in the queue for the given fitting from a
%%      vnode.  Note: this should only be called by the worker process
%%      for that fitting-vnode pair.  This will cause the vnode to
%%      send everything that is in the queue (non-blocking) to the
%%      worker process for this fitting.
-spec next_input_list(pid(), riak_pipe:fitting()) -> ok.
next_input_list(Pid, Fitting) ->
    riak_core_vnode:send_command(
      Pid, #cmd_next_input{fitting=Fitting, type=list}).

%% @doc Send the result of archiving a worker to the vnode that owns
%%      that worker.  Note: this should only be called by the worker
%%      being archived.  This will cause the vnode to send that
%%      worker's queue and archive to its handoff partner when
%%      instructed to do so.
-spec reply_archive(pid(), riak_pipe:fitting(), term()) -> ok.
reply_archive(Pid, Fitting, Archive) ->
    riak_core_vnode:send_command(Pid, #cmd_archive{fitting=Fitting,
                                                   archive=Archive}).

%% @doc Get some information about the worker queues on this vnode.
%%      The result is a tuple of the form `{PartitionNumber,
%%      [WorkerProplist]}'.  Each WorkerProplist contains tagged
%%      tuples, such as:
%%<dl><dt>
%%      `fitting'
%%</dt><dd>
%%      The pid of the fitting the worker implements.
%%</dd><dt>
%%      `name'
%%</dt><dd>
%%      The name of the fitting.
%%</dd><dt>
%%      `module'
%%</dt><dd>
%%      The module that implements the fitting.
%%</dd><dt>
%%      `state'
%%</dt><dd>
%%      The state of the worker.  One of `working', `waiting', `init'.
%%</dd><dt>
%%      `inputs_done'
%%</dt><dd>
%%      Boolean: true if `eoi' has been delivered for this fitting,
%%      false otherwise.
%%</dd><dt>
%%      `queue_length'
%%</dt><dd>
%%      Integer number of items in the worker's queue.
%%</dd><dt>
%%      `blocking_length'
%%</dt><dd>
%%      Integer number of requests blocking on the queue.
%%</dd><dt>
%%      `started'
%%</dt><dd>
%%      An {@link erlang:now/0} tuple, indicating the time that the
%%      worker started.
%%</dd><dt>
%%      `processed'
%%</dt><dd>
%%      Integer number of inputs that the worker has processed.
%%</dd><dt>
%%      `failures'
%%</dt><dd>
%%      Integer number of times that the worker has failed (and was
%%      restarted).
%%</dd><dt>
%%      `work_time'
%%</dt><dd>
%%      Total time the worker has spent processing inputs (as opposed
%%      to waiting, idle for them).  Given as an integer number of
%%      microseconds.
%%</dd><dt>
%%      `idle_time'
%%</dt><dd>
%%      Total time the worker has spent waiting for inputs (as opposed
%%      to working on them).  Given as an integer number of
%%      microseconds.  Should be roughly equal to
%%      `(now()-started)-work_time'.
%%</dd></dl>
-spec status(pid()) -> {partition(), [[{atom(), term()}]]}.
status(Pid) ->
    status(Pid, all).

%% @doc Produces the same type of data as {@link status/1}, but only
%%      includes information for the fittings given.
-spec status(pid(), [#fitting{}] | all)
         -> {partition(), [[{atom(), term()}]]}.
status(Pid, Fittings) when is_list(Fittings); Fittings =:= all ->
    Ref = make_ref(),
    riak_core_vnode:send_command(Pid, #cmd_status{sender={raw, Ref, self()},
                                                  fittings=Fittings}),
    receive
        {Ref, Reply} -> Reply
    end.
    

%% @doc Handle a vnode command.
-spec handle_command(term(), sender(), state()) ->
          {reply, term(), state()}
        | {noreply, state()}.
handle_command(ping, _Sender, State) ->
    {reply, {pong, State#state.partition}, State};
handle_command(#cmd_enqlist{}=Cmd, Sender, State) ->
    enqueue_internal(Cmd, Sender, State);
handle_command(#cmd_enqueue{}=Cmd, Sender, State) ->
    enqueue_internal(Cmd, Sender, State);
handle_command(#cmd_eoi{}=Cmd, _Sender, State) ->
    eoi_internal(Cmd, State);
handle_command(#cmd_next_input{}=Cmd, _Sender, State) ->
    next_input_internal(Cmd, State);
handle_command(#cmd_status{}=Cmd, _Sender, State) ->
    status_internal(Cmd, State);
handle_command(Message, _Sender, State) ->
    lager:info("Unhandled command: ~p", [Message]),
    {noreply, State}.

%% @doc Handle a handoff command.
-spec handle_handoff_command(term(), sender(), state()) ->
         {reply, term(), state()}
       | {noreply, state()}
       | {forward, state()}.
handle_handoff_command(?FOLD_REQ{}=Cmd, Sender, State) ->
    handoff_cmd_internal(Cmd, Sender, State);
handle_handoff_command(#cmd_archive{}=Cmd, _Sender, State) ->
    archive_internal(Cmd, State);
handle_handoff_command(#cmd_enqueue{fitting=F}=Cmd, Sender,
                       #state{handoff=#handoff{}}=State) ->
    case worker_by_fitting(F, State) of
        {ok, _} ->
            %% not yet handed off: proceed
            handle_command(Cmd, Sender, State);
        none ->
            %% handed off, or never existed: forward
            {forward, State}
    end;
handle_handoff_command(#cmd_eoi{fitting=F}=Cmd, Sender,
                       #state{handoff=#handoff{}}=State) ->
    case worker_by_fitting(F, State) of
        {ok, _} ->
            %% not yet handed off: proceed
            handle_command(Cmd, Sender, State);
        none ->
            %% handed off, or never existed: reply done
            %% (let the other node deal with its own eoi)
            send_done(F),
            {noreply, State}
    end;
handle_handoff_command(#cmd_next_input{fitting=F}, _Sender,
                       #state{handoff=#handoff{}}=State) ->
    %% force workers into waiting state so we can ask them to
    %% prepare for handoff
    {noreply, archive_fitting(F, State)};
handle_handoff_command(Cmd, Sender, State) ->
    %% handle the rest (#cmd_status, Unknown) as usual
    handle_command(Cmd, Sender, State).

%% @doc Be prepared to handoff.
-spec handoff_starting(node(), state()) -> {true, state()}.
handoff_starting(_TargetNode, State) ->
    {true, State#state{handoff=starting}}.

%% @doc Stop handing off before getting started.
-spec handoff_cancelled(state()) -> {ok, state()}.
handoff_cancelled(#state{handoff=starting, workers_archiving=[]}=State) ->
    %%TODO: handoff is only cancelled before anything is handed off, right?
    {ok, State#state{handoff=cancelled}}.

%% @doc Note that handoff has completed.
-spec handoff_finished(node(), state()) -> {ok, state()}.
handoff_finished(_TargetNode, #state{workers=[]}=State) ->
    %% #state.workers should be empty, because they were all handed off
    %% clear out list of handed off items
    {ok, State#state{handoff=finished}}.

%% @doc Accept handoff data from some other node.  `Data' should be a
%%      term_to_binary-ed `#worker_handoff{}' record.  See {@link
%%      encode_handoff_item/2}.
%%
%%      Ensure that a worker is running for the fitting, merge queues,
%%      and prepare to handle archive transfer.
-spec handle_handoff_data(binary(), state()) ->
         {reply, ok | {error, term()}, state()}.
handle_handoff_data(Data, State) ->
    #worker_handoff{fitting=Fitting,
                    queue=Queue,
                    blocking=Blocking,
                    archive=Archive} = binary_to_term(Data),
    case worker_for(Fitting, false, State) of
        {ok, Worker} ->
            NewWorker = handoff_worker(Worker, Queue, Blocking, Archive),
            {reply, ok, replace_worker(NewWorker, State)};
        Error ->
            {reply, {error, Error}, State}
    end.

%% @doc Produce a binary representing the worker data to handoff.
-spec encode_handoff_item(riak_pipe:fitting(),
                          {queue(), queue(), term()}) ->
         binary().
encode_handoff_item(Fitting, {Queue, Blocking, Archive}) ->
    term_to_binary(#worker_handoff{fitting=Fitting,
                                   queue=Queue,
                                   blocking=Blocking,
                                   archive=Archive}).

%% @doc Determine whether this vnode has any running workers.
-spec is_empty(state()) -> {boolean(), state()}.
is_empty(#state{workers=Workers}=State) ->
    {Workers==[], State}.

%% @doc Unused.
-spec delete(state()) -> {ok, state()}.
delete(#state{workers=[]}=State) ->
    %%TODO: delete is only called if is_empty/1==true, right?
    {ok, State}.

%% @doc Unused.
-spec terminate(term(), state()) -> ok.
terminate(_Reason, _State) ->
    ok.

%% @doc Handle an 'EXIT' message from a worker process.
%%
%%      If the worker died normally after receiving end-of-inputs and
%%      emptying its queue, send `done' to the fitting, and remove the
%%      worker's entry in the vnodes list.
%%
%%      If the worker died abnormally, attempt to restart it.
-spec handle_exit(pid(), term(), state()) -> {noreply, state()}.
handle_exit(Pid, Reason, #state{partition=Partition}=State) ->
    NewState = case worker_by_pid(Pid, State) of
                   {ok, Worker} ->
                       case {Worker#worker.inputs_done,
                             queue:is_empty(Worker#worker.q),
                             Reason} of
                           {true, true, normal} ->
                               ?T(Worker#worker.details, [done],
                                  {vnode, {done, Partition,
                                           proplist_perf(Worker)}}),
                               send_done(Worker#worker.fitting),
                               remove_worker(Worker, State);
                           {true, false, normal} ->
                               %% inputs arrived between asking the worker
                               %% to shutdown, and it shutting down;
                               %% silently restart
                               restart_worker(Worker, State);
                           _ ->
                               if Reason /= processing_error ->
                                       %% the sink has not yet been
                                       %% alerted of this input's failure
                                       worker_error(Reason, Worker, State);
                                  true -> ok
                               end,
                               restart_worker(inc_fail_perf(Worker), State)
                       end;
                   none ->
                       %% TODO: log this somewhere?
                       %% don't know what this pid is
                       %% may be old worker EXIT passing in flight
                       State
               end,
    {noreply, NewState}.

%% @doc Handle a 'DOWN' message from a fitting process. Kill the
%%      worker associated with that fitting and dispose of its queue.
-spec handle_info(term(), state()) -> {ok, state()}.
handle_info({'DOWN',_,process,Pid,_},
            #state{partition=Partition, worker_sup=WorkerSup}=State) ->
    NewState = case worker_by_fitting_pid(Pid, State) of
                   {ok, Worker} ->
                       ?T(Worker#worker.details, [error],
                          {vnode, {fitting_died, Partition}}),
                       %% if the fitting died, tear down its worker
                       erlang:unlink(Worker#worker.pid),
                       riak_pipe_vnode_worker_sup:terminate_worker(
                         WorkerSup, Worker#worker.pid),
                       remove_worker(Worker, State);
                   none ->
                       %% TODO: log this somewhere?
                       %% don't know what this pid is
                       %% may be old worker DOWN passing in flight
                       State
               end,
    {ok, NewState};
handle_info(_,State) ->
    %% unknown message
    {ok, State}.

%% @doc Coverage requests may be used to enqueue identical work on
%%      multiple vnodes.  `Input' is delivered to the worker as
%%      `{cover, FilterVNodes, Input}'.
-spec handle_coverage(term(), term(), sender(), state()) ->
         {reply, ok, state()}.
handle_coverage({Fitting, Input}, FilterVNodes, Sender,
                #state{partition=Partition}=State) ->
    CoverInput = {cover, FilterVNodes, Input},
    Cmd = #cmd_enqueue{fitting=Fitting, input=CoverInput,
                       timeout=infinity,
                       usedpreflist=[{Partition, node()}]},
    handle_command(Cmd, Sender, State);
handle_coverage(_Request, _KeySpaces, _Sender, State) ->
    {reply, ok, State}.

%% internal

%% @doc Handle a command to add an input to the work queue.  This
%%      function ensures there is a worker running for the fitting (if
%%      there is room within `worker_limit').  It then adds the input
%%      to the queue and replies `ok' if the queue is below capacity
%%      (`worker_queue_limit').  If the queue is at capacity, the
%%      request is added to the blocking queue, and no reponse is sent
%%      (until later, when the input is moved from the blocking queue
%%      to the work queue).
-spec enqueue_internal(#cmd_enqueue{}, sender(), state()) ->
         {reply,
          ok | {error, qerror()},
          state()}
       | {noreply, state()}.
enqueue_internal(#cmd_enqueue{fitting=Fitting, input=Input, timeout=TO,
                              usedpreflist=UsedPreflist},
                 Sender, State) ->
    enqueue_internal(Fitting, {single, [Input]},
                     TO, UsedPreflist, Sender, State);
enqueue_internal(#cmd_enqlist{fitting=Fitting, inputs=Inputs, timeout=TO,
                              usedpreflist=UsedPreflist},
                 Sender, State) ->
    enqueue_internal(Fitting, {list, Inputs},
                     TO, UsedPreflist, Sender, State).

enqueue_internal(Fitting, Input, TO, UsedPreflist,
                 Sender, #state{partition=Partition}=State) ->
    case worker_for(Fitting, true, State) of
        {ok, #worker{details=#fitting_details{module=riak_pipe_w_crash}}}
          when Input == {single, [vnode_killer]} ->
            %% this is used by the eunit test named "Vnode Death"
            %% in riak_pipe:exception_test_; it kills the vnode before
            %% it has a chance to reply to the queue request
            exit({riak_pipe_w_crash, vnode_killer});
        {ok, Worker} when (Worker#worker.details)#fitting_details.module
                          /= ?FORWARD_WORKER_MODULE ->
            case add_input(Worker, Partition, Input,
                           Sender, TO, UsedPreflist) of
                {ok, NewWorker} ->
                    {reply, qreply(Input, ok),
                     replace_worker(NewWorker, State)};
                {queue_full, NewWorker} ->
                    %% if the queue is full, hold up the producer
                    %% until we're ready for more
                    {noreply, replace_worker(NewWorker, State)};
                {timeout, NewWorker, Accepted} ->
                    {reply, qreply(Input, {error, {timeout, Accepted}}),
                     replace_worker(NewWorker, State)}
            end;
        {ok, _RestartForwardingWorker} ->
            %% this is a forwarding worker for a failed-restart
            %% fitting - don't enqueue any more inputs, just reject
            %% and let the requester enqueue elswhere
            {reply, qreply(Input, {error, forwarding}), State};
        worker_limit_reached ->
            %% TODO: log/trace this event
            %% Except we don't have details here to associate with a trace
            %% function: ?T_ERR(WhereToGetDetails, whatever_limit_hit_here),
            {reply, qreply(Input, {error, worker_limit_reached}), State};
        worker_startup_failed ->
            %% TODO: log/trace this event
            {reply, qreply(Input, {error, worker_startup_failed}), State}
    end.

%% @doc Create the correct type of reply for an enqueue
%% request. Single-item requests just want the reply, primarily for
%% compatibility with 1.2 and earlier nodes. List requests want the
%% reply and where it came from, in order to multiplex input
%% processing.
-spec qreply(Input::{single|list, term()}, term()) ->
        {pid(), term()} | term().
qreply({single, _}, {error, {timeout, _}}) ->
    timeout;
qreply({single, _}, Reply) ->
    Reply;
qreply({list, _}, Reply) ->
    {self(), Reply}.

%% @doc Find the worker for the given `Fitting', or start one if there
%%      is room on this vnode.  Returns `{ok, Worker}' if a worker
%%      [now] exists, or `worker_limit_reached' otherwise.
-spec worker_for(#fitting{}, boolean(), state()) ->
         {ok, #worker{}} | worker_limit_reached | worker_startup_failed.
worker_for(Fitting, EnforceLimitP,
           #state{workers=Workers, worker_limit=Limit}=State) ->
    case worker_by_fitting(Fitting, State) of
        {ok, Worker} ->
            {ok, Worker};
        none ->
            if (not EnforceLimitP) orelse length(Workers) < Limit ->
                    new_worker(Fitting, State);
               true ->
                    worker_limit_reached
            end
    end.

%% @doc Start a new worker for the given `Fitting'.  This function
%%      requests the details from the fitting process, monitors the
%%      fitting process, starts and links the worker process, and sets
%%      up the queues for it.
-spec new_worker(riak_pipe:fitting(), state()) ->
         {ok, #worker{}} | worker_startup_failed.
new_worker(Fitting, #state{partition=P, worker_sup=Sup, worker_q_limit=WQL}) ->
    try
        case riak_pipe_fitting:get_details(Fitting, P) of
            {ok, #fitting_details{q_limit=FQL}=Details} ->
                erlang:monitor(process, Fitting#fitting.pid),
                {ok, Pid} = riak_pipe_vnode_worker_sup:start_worker(
                              Sup, Details),
                erlang:link(Pid),
                Start = now(),
                Perf = #worker_perf{started=Start, last_time=Start},
                ?T(Details, [worker], {vnode, {start, P}}),
                {ok, #worker{pid=Pid,
                             fitting=Fitting,
                             details=Details,
                             state=init,
                             inputs_done=false,
                             q=queue:new(),
                             q_limit=lists:min([WQL, FQL]),
                             blocking=queue:new(),
                             perf=Perf}};
            gone ->
                lager:error(
                  "Pipe worker startup failed:"
                  "fitting was gone before startup"),
                worker_startup_failed
        end
    catch Type:Reason ->
            lager:error(
              "Pipe worker startup failed:~n"
              "   ~p:~p~n   ~p",
              [Type, Reason, erlang:get_stacktrace()]),
            worker_startup_failed
    end.

%% @doc Start a new worker to forward inputs for the given `Fitting'.
%%      This is only used when a worker failed, and then also failed
%%      to restart.  The worker created here simply clears the
%%      existing queue for forwarding all of the inputs to the next
%%      vnode in their preflist.
-spec new_fwd_worker(riak_pipe_fitting:details(), state()) ->
         {ok, #worker{}}.
new_fwd_worker(FittingDetails,
               #state{partition=P, worker_sup=Sup, worker_q_limit=WQL}) ->
    %% Override the fitting's normal behavior,
    %% and force it to just forward inputs
    ForwardDetails = FittingDetails#fitting_details{
                       module=?FORWARD_WORKER_MODULE},
    {ok, Pid} = riak_pipe_vnode_worker_sup:start_worker(
                  Sup, ForwardDetails),
    erlang:link(Pid),
    Start = now(),
    Perf = #worker_perf{started=Start, last_time=Start},
    ?T(FittingDetails, [fwd_worker], {vnode, {start, P}}),
    {ok, #worker{pid=Pid,
                 fitting=ForwardDetails#fitting_details.fitting,
                 details=ForwardDetails,
                 state=init,
                 inputs_done=false,
                 q=queue:new(),
                 q_limit=WQL,
                 blocking=queue:new(),
                 perf=Perf}}.

%% @doc A fun to send 'queued' traces as multiple inputs are added to
%% the queue.
trace_queue(Details, Partition) ->
    fun(Input) ->
            ?T(Details, [queue], {vnode, {queued, Partition, Input}})
    end.

%% @doc Add an input to the worker's queue.  If the worker is
%%      `waiting', send the input to it, skipping the queue.  If the
%%      queue is full, add the request to the blocking queue instead.
-spec add_input(#worker{}, partition(),
                {single | list, list()}, sender(), qtimeout(),
                riak_core_apl:preflist()) ->
         {ok | queue_full, #worker{}} | timeout.
add_input(#worker{state={waiting, one}, details=D}=Worker, Partition,
          {Type, [Input|Rest]}, Sender, TO, UsedPreflist) ->
    %% worker has been waiting for the first thing to enter its queue
    (trace_queue(D, Partition))(Input),
    send_input(Worker, {Input, UsedPreflist}),
    PerfWorker = roll_perf(Worker),
    case add_input(PerfWorker#worker{state={working, Input}}, Partition,
                   {Type, Rest}, Sender, TO, UsedPreflist) of
        {timeout, NewWorker, Accepted} ->
            {timeout, NewWorker, Accepted+1};
        Other ->
            Other
    end;
add_input(#worker{state={waiting, list},q=Q,q_limit=QL,details=D}=Worker,
          Partition,
          {Type, Inputs}, Sender, TO, UsedPreflist) ->
    %% worker has been waiting for things to enter its queue;
    %% send as much as would fit in its queue, then enqueue the rest
    {Accepted, Rest, NewQ} = queue_n(QL, Inputs, UsedPreflist, Q,
                                     trace_queue(D, Partition)),
    send_input(Worker, queue:to_list(NewQ)),
    PerfWorker = roll_perf(Worker),
    case add_input(PerfWorker#worker{state={working, Inputs}}, Partition,
                   {Type, Rest}, Sender, TO, UsedPreflist) of
        {timeout, NewWorker, NewAccepted} ->
            {timeout, NewWorker, NewAccepted+Accepted};
        Other ->
            Other
    end;
add_input(#worker{q=Q, q_limit=QL, blocking=Blocking, details=D}=Worker,
          Partition, {Type, Inputs}, Sender, TO, UsedPreflist) ->
    case queue_n(QL - queue:len(Q), Inputs, UsedPreflist, Q,
                 trace_queue(D, Partition)) of
        {_, [], NewQ} ->
            {ok, Worker#worker{q=NewQ}};
        {_, Rest, NewQ} when TO =/= noblock ->
            %% send each input as a separate trace, for
            %% compatibility with 1.2-and-earlier trace loggers
            [?T(Worker#worker.details, [queue,queue_full],
                {vnode, {queue_full, Partition, I}})
             || I <- Rest],
            NewBlocking = queue:in({{Type, Rest}, Sender, UsedPreflist},
                                   Blocking),
            {queue_full, Worker#worker{q=NewQ, blocking=NewBlocking}};
        {Accepted, _, NewQ} ->
            {timeout, Worker#worker{q=NewQ}, Accepted}
    end.

%% @doc Queue `N' items from list `L' with used-preflist `UP', into
%% queue `Q', tracing this queing with function `T'. Returns the
%% number of items queued, `C' =&lt; `N', the remainder of `L', and
%% the new queue.
-spec queue_n(non_neg_integer(),
              list(),
              riak_core_apl:preflist(),
              queue(),
              fun((_) -> any()))
         -> {Accepted::non_neg_integer(),
             Remaining::list(),
             Queue::queue()}.
queue_n(N, L, UP, Q, T) ->
    queue_n(N, L, UP, Q, 0, T).
queue_n(0, L, _UP, Q, C, _T) ->
    {C, L, Q};
queue_n(_N, []=L, _UP, Q, C, _T) ->
    {C, L, Q};
queue_n(N, [H|L], UP, Q, C, T) ->
    T(H), %% trace function hook
    queue_n(N-1, L, UP, queue:in({H,UP},Q), C+1, T).

%% @doc Merge the worker on this vnode with the worker from another
%%      vnode.  (The grungy part of {@link handle_handoff_data/2}.)
-spec handoff_worker(#worker{}, queue(), queue(), Archive::term()) ->
          #worker{}.
handoff_worker(#worker{q=Q, blocking=Blocking}=Worker,
               HandoffQ, HandoffBlocking, HandoffState) ->
    %% simply concatenate queues, and hold the handoff state for
    %% the next available time to ask the worker to deal with it
    MergedWorker = Worker#worker{
                     q=queue:join(Q, HandoffQ),
                     blocking=queue:join(Blocking, HandoffBlocking),
                     handoff={waiting, HandoffState}},
    maybe_wake_for_handoff(MergedWorker).

%% @doc If the worker is `waiting', send it the handoff data to
%%      process.  Otherwise, just leave it be until it asks for the
%%      next input.
-spec maybe_wake_for_handoff(#worker{}) -> #worker{}.
maybe_wake_for_handoff(#worker{state={waiting, _}}=Worker) ->
    send_handoff(Worker),
    Worker#worker{state={working, handoff}, handoff=undefined};
maybe_wake_for_handoff(Worker) ->
    %% worker is doing something else - send handoff later
    Worker.

%% @doc Handle an end-of-inputs command.  If the worker for the given
%%      fitting is `waiting', ask it to shutdown immediately.
%%      Otherwise, mark that eoi was received, and ask the worker to
%%      shut down when it empties its queue.
-spec eoi_internal(#cmd_eoi{}, state()) -> {noreply, state()}.
eoi_internal(#cmd_eoi{fitting=Fitting}, #state{partition=Partition}=State) ->
    NewState = case worker_by_fitting(Fitting, State) of
                   {ok, Worker} ->
                       case Worker#worker.state of
                           {waiting, _} ->
                               ?T(Worker#worker.details, [eoi],
                                  {vnode, {eoi, Partition}}),
                               send_input(Worker, done),
                               replace_worker(
                                 Worker#worker{state={working, done},
                                               inputs_done=true},
                                 State);
                           _ ->
                               replace_worker(
                                 Worker#worker{inputs_done=true},
                                 State)
                       end;
                       %% send_done(Fitting) should go in 'DOWN' handle
                   none ->
                       %% that worker never existed,
                       %% or the 'DOWN' messages are passing in flight
                       send_done(Fitting),
                       State
               end,
    {noreply, NewState}.

%% @doc Handle a request from a worker for its next input.
%%
%%      If this vnode is handing data off, ask the worker to archive.
%%
%%      If this vnode is not handing off, send the next input.
-spec next_input_internal(#cmd_next_input{}, state()) ->
          {noreply, #state{}}.
next_input_internal(#cmd_next_input{fitting=Fitting, type=Type}, State) ->
    case worker_by_fitting(Fitting, State) of
        {ok, #worker{handoff=undefined}=Worker} ->
            next_input_nohandoff(Worker, Type, State);
        {ok, Worker} ->
            send_handoff(Worker),
            HandoffWorker = Worker#worker{state={working, handoff},
                                          handoff=undefined},
            {noreply, replace_worker(HandoffWorker, State)};
        none ->
            %% this next_input request was for a queue that this vnode
            %% doesn't have.  ignore it.  (one example is if the vnode
            %% receives a 'DOWN' for a fitting, and cleans up the
            %% queue for that fitting's worker *after* the worker has
            %% requested its next input, but before the vnode has
            %% received that request)
            {noreply, State}
    end.

%% @doc Handle pulling data off of a worker's queue and sending it to
%%      the worker.
%%
%%      If there are no inputs in the worker's queue, mark the worker
%%      as `waiting' if it has not yet received its end-of-inputs
%%      message, or ask it to shutdown if eoi was received.
%%
%%      If there are inputs in the queue, pull off the front one and
%%      send it along.  If there are items in the blocking queue, move
%%      the front one to the end of the work queue, and reply `ok' to
%%      the process that requested its addition (unblocking it).
-spec next_input_nohandoff(#worker{}, nitype(), state()) ->
         {noreply, state()}.
next_input_nohandoff(WorkerUnperf,
                     Type,
                     #state{partition=Partition}=State) ->
    Worker = roll_perf(WorkerUnperf),
    case niqout(Type, Worker#worker.q, Worker#worker.details, Partition) of
        {Input, NewQ} ->
            send_input(Worker, Input),
            WorkingWorker = Worker#worker{state={working, Input},
                                          q=NewQ},
            FinalWorker = maybe_unblock(Worker#worker.q_limit-queue:len(NewQ),
                                        WorkingWorker, Partition);
        empty ->
            FinalWorker = maybe_done(Worker, Type, Partition)
    end,
    {noreply, replace_worker(FinalWorker, State)}.

%% @doc Pull things out of the blocking queue if there is room in the
%% input queue. First parameter is the maximum number of items to unblock.
-spec maybe_unblock(non_neg_integer(), #worker{}, partition()) -> #worker{}.
maybe_unblock(L, Worker, _Partition) when L =< 0 ->
    Worker;
maybe_unblock(L, #worker{q=Q, blocking=B, details=D}=Worker, Partition) ->
    case queue:out(B) of
        {{value, {{Type,BlockInputs}=BI,
                  Blocker, BlockUsedPreflist}},
         NewB} ->
            Trace = fun(_) ->
                            ?T(D, [queue,queue_full],
                               {vnode, {unblocking, Partition}})
                    end,
            {Accepted, Rest, NewQ} = queue_n(L, BlockInputs,
                                             BlockUsedPreflist,
                                             Q, Trace),
            case Rest of
                [] ->
                    %% finished queueing for this request,
                    %% free up blocked sender
                    reply_to_blocker(Blocker, BI, ok),
                    maybe_unblock(L-Accepted,
                                  Worker#worker{q=NewQ, blocking=NewB},
                                  Partition);
                _ ->
                    %% still more items to queue for this request,
                    %% continue blocking
                    StillB = queue:in_r({{Type, Rest},
                                         Blocker,
                                         BlockUsedPreflist},
                                        NewB),
                    Worker#worker{q=NewQ, blocking=StillB}
            end;
        {empty, _} ->
            %% nothing blocking - just continue
            Worker
    end.

%% @doc Send the `done' signal to the worker if we have received eoi
%% for it.
-spec maybe_done(#worker{}, nitype(), partition()) -> #worker{}.
maybe_done(#worker{inputs_done=true}=Worker, _Type, Partition) ->
    ?T(Worker#worker.details, [eoi], {vnode, {eoi, Partition}}),
    send_input(Worker, done),
    Worker#worker{state={working, done}};
maybe_done(#worker{inputs_done=false}=Worker, Type, Partition) ->
    ?T(Worker#worker.details, [queue], {vnode, {waiting, Partition}}),
    Worker#worker{state={waiting, Type}}.

%% @doc Prepare the correct "next input" response type, depending on
%% whether the worker requested one input, or the whole queue as a
%% list.
-spec niqout(nitype(), queue(), #fitting_details{}, partition()) ->
         {Input::term(), NewQueue::queue()} | empty.
niqout(one, Q, Details, Partition) ->
    case queue:out(Q) of
        {{value, I}, NewQ} ->
            ?T(Details, [queue], {vnode, {dequeue, Partition}}),
            {I, NewQ};
        {empty, _} ->
            empty
    end;
niqout(list, Q, Details, Partition) ->
    case queue:to_list(Q) of
        [] ->
            empty;
        L ->
            ?T(Details, [queue], {vnode, {dequeue_list, Partition}}),
            {L, queue:new()}
    end.

%% @doc Send an input to a worker.
-spec send_input(#worker{},
                 done | {term(), riak_core_apl:preflist()}) ->
         ok.
send_input(Worker, Input) ->
    riak_pipe_vnode_worker:send_input(Worker#worker.pid, Input).

%% @doc Send an request to archive to a worker.
-spec send_archive(#worker{}) -> ok.
send_archive(Worker) ->
    riak_pipe_vnode_worker:send_archive(Worker#worker.pid).

%% @doc Send a request to merge another node's archived worker state
%%      with this worker.
-spec send_handoff(#worker{}) -> ok.
send_handoff(#worker{handoff={waiting, HO}}=Worker) ->
    riak_pipe_vnode_worker:send_handoff(Worker#worker.pid, HO).

%% @doc Find a worker by its pid.
-spec worker_by_pid(pid(), state()) -> {ok, #worker{}} | none.
worker_by_pid(Pid, #state{workers=Workers}) ->
    case lists:keyfind(Pid, #worker.pid, Workers) of
        #worker{}=Worker -> {ok, Worker};
        false            -> none
    end.

%% @doc Find a worker by the fitting it works for.
-spec worker_by_fitting(riak_pipe:fitting(), state()) ->
         {ok, #worker{}} | none.
worker_by_fitting(Fitting, #state{workers=Workers}) ->
    case lists:keyfind(Fitting, #worker.fitting, Workers) of
        #worker{}=Worker -> {ok, Worker};
        false            -> none
    end.

%% @doc Find a worker by the pid of the fitting it works for.
-spec worker_by_fitting_pid(pid(), state()) -> {ok, #worker{}} | none.
worker_by_fitting_pid(Pid, #state{workers=Workers}) ->
    case [ W || #worker{fitting=F}=W <- Workers, F#fitting.pid =:= Pid ] of
        [#worker{}=Worker] -> {ok, Worker};
        []                 -> none
    end.

%% @doc Update the worker's entry in the vnode's state.  Matching
%%      is done by fitting.
-spec replace_worker(#worker{}, state()) -> state().
replace_worker(#worker{fitting=F}=Worker, #state{workers=Workers}=State) ->
    NewWorkers = lists:keystore(F, #worker.fitting, Workers, Worker),
    State#state{workers=NewWorkers}.

%% @doc Remove the worker's entry from the vnode's state.  Matching is
%%      done by fitting.
-spec remove_worker(#worker{}, state()) -> state().
remove_worker(#worker{fitting=F}, #state{workers=Workers}=State) ->
    NewWorkers = lists:keydelete(F, #worker.fitting, Workers),
    State#state{workers=NewWorkers}.

%% @doc Restart the worker after failure.  The input that the worker
%%      was processing is skipped.  If the worker fails to restart,
%%      the inputs in its work queue are sent to the fitting process,
%%      and the requests in its block queue are sent `{error, fail}'
%%      responses.
-spec restart_worker(#worker{}, state()) -> state().
restart_worker(#worker{details=FD}=UnstatWorker,
               #state{partition=Partition}=State)
  when FD#fitting_details.module /= ?FORWARD_WORKER_MODULE ->
    Worker = roll_perf(UnstatWorker),
    CleanState = remove_worker(Worker, State),
    case new_worker(Worker#worker.fitting, CleanState) of
        {ok, NewWorker} ->
            ?T(Worker#worker.details, [restart],
               {vnode, {restart, Partition}}),
            CopiedWorker = NewWorker#worker{
                             q=Worker#worker.q,
                             blocking=Worker#worker.blocking,
                             inputs_done=Worker#worker.inputs_done,
                             perf=Worker#worker.perf},
            replace_worker(CopiedWorker, CleanState);
        _Error ->
            ?T(Worker#worker.details, [restart_fail],
               {vnode, {restart_fail, Partition, proplist_perf(Worker)}}),
            %% fail blockers, so they resubmit elsewhere
            [ reply_to_blocker(Blocker, BI, {error, worker_restart_fail})
              || {BI, Blocker, _} <- queue:to_list(Worker#worker.blocking) ],
            %% spin up a stub worker to forward the inputs
            %% (don't want to tie up the vnode doing this sending)
            {ok, FwdWorker} = new_fwd_worker(Worker#worker.details,
                                             CleanState),
            %% an alternate config might be to set inputs_done to
            %% true, unconditionally, so the worker is cleaned up as
            %% soon as it is done forwarding; but if the vnode gets
            %% another input for this fitting after recycling the
            %% worker, it will try to restart it again, which is
            %% likely to fail again, and such repeated restarts could
            %% lead to a heavy load on this vnode
            CopiedWorker = FwdWorker#worker{
                             q=Worker#worker.q,
                             inputs_done=Worker#worker.inputs_done},
            replace_worker(CopiedWorker, CleanState)
    end;
restart_worker(#worker{details=FD, q=Queue}=Worker,
               #state{partition=Partition}=State) ->
    ?T(FD, [restart_fail], {vnode, {restart_fail, Partition}}),
    %% this was a forwarding worker for a failed-restart fitting; if
    %% it crashed, there's something *really* wrong - log the errors
    %% and dump it
    [ ?T_ERR(FD, {restart_dropped, I}) || I <- queue:to_list(Queue) ],
    if Worker#worker.inputs_done ->
            %% tell the fitting this worker has exited, so it doesn't
            %% hang around waiting
            send_done(FD#fitting_details.fitting);
       true ->
            %% the fitting hasn't yet sent eoi - let the done message
            %% be sent after that happens
            ok
    end,
    remove_worker(Worker, State).

worker_error(Reason, #worker{details=FD}=Worker, State) ->
    Fields = record_info(fields, fitting_details),
    FieldPos = lists:zip(Fields, lists:seq(2, length(Fields)+1)),
    DsList = [{Field, element(Pos, FD)} || {Field, Pos} <- FieldPos],
    ?T_ERR(FD, [{module, FD#fitting_details.module},
                {partition, State#state.partition},
                {details, DsList},
                {reason, Reason},
                {state, Worker#worker.state}]).

%% @doc Reply to a request that has been waiting in a worker's blocked
%%      queue.
-spec reply_to_blocker(term(), {single|list, term}, term()) -> true.
reply_to_blocker(Blocker, Input, Reply) ->
    riak_core_vnode:reply(Blocker, qreply(Input, Reply)).

%% @doc Send a `done' message to the fitting specified.
-spec send_done(riak_pipe:fitting()) -> ok.
send_done(Fitting) ->
    riak_pipe_fitting:worker_done(Fitting).

%% @doc Handle a request for status.  Generate the worker detail
%%      list, and send it to the requester.
-spec status_internal(#cmd_status{}, state()) -> {noreply, state()}.
status_internal(#cmd_status{sender=Sender, fittings=Fittings},
                #state{partition=P, workers=Workers}=State) ->
    FilteredWorkers =
        case Fittings of
            all ->
                Workers;
            _ ->
                [ W || W <- Workers,
                       lists:member(W#worker.fitting, Fittings)]
        end,
    Reply = {P, [ worker_detail(W) || W <- FilteredWorkers]},
    %% riak_core_vnode:command(Pid) does not set reply properly
    riak_core_vnode:reply(Sender, Reply),
    {noreply, State}.

%% @doc Generate the status details for the given worker.
-spec worker_detail(#worker{}) -> [{atom(), term()}].
worker_detail(#worker{fitting=Fitting, details=Details,
                      state=State, inputs_done=Done,
                      q=Q, blocking=B}=Worker) ->
    [{fitting, Fitting#fitting.pid},
     {name, Details#fitting_details.name},
     {module, Details#fitting_details.module},
     {state, case State of
                 {working, _} -> working;
                 Other        -> Other
             end},
     {inputs_done, Done},
     {queue_length, queue:len(Q)},
     {blocking_length, queue:len(B)}
     |proplist_perf(Worker)].

%% @doc Update the appropriate fields in the worker's performance
%%      statistics.  This function should be called before updating
%%      the worker to its next state, as the function depends on
%%      `#worker.state' to know which fields to update.  That is, if
%%      the worker has just finished processing input A, this function
%%      should be called while its state is still set to `{working, A}'.
-spec roll_perf(#worker{}) -> #worker{}.
roll_perf(#worker{perf=Perf, state=State}=Worker) ->
    Now = now(),
    Duration = timer:now_diff(Now, Perf#worker_perf.last_time),
    TimedPerf = case State of
                    {working,_} ->
                        Perf#worker_perf{
                          processed=Perf#worker_perf.processed+1,
                          work_time=Perf#worker_perf.work_time+Duration};
                    _ ->
                        Perf#worker_perf{
                          idle_time=Perf#worker_perf.idle_time+Duration}
                end,
    Worker#worker{perf=TimedPerf#worker_perf{last_time=Now}}.

%% @doc Increment the failure counter in this worker's performance
%%      statistics.
-spec inc_fail_perf(#worker{}) -> #worker{}.
inc_fail_perf(#worker{perf=Perf}=Worker) ->
    FailPerf = Perf#worker_perf{failures=1+Perf#worker_perf.failures},
    Worker#worker{perf=FailPerf}.
    
%% @doc Convert the worker's performance statistics to a proplist, for
%%      sharing.
-spec proplist_perf(#worker{}) -> [{atom(), term()}].
proplist_perf(#worker{perf=Perf, state=State}) ->
    SinceLast = timer:now_diff(now(), Perf#worker_perf.last_time),
    {AddWork, AddIdle} = case State of
                             {working, _} -> {SinceLast, 0};
                             _            -> {0, SinceLast}
                         end,
    [{started, Perf#worker_perf.started},
     {processed, Perf#worker_perf.processed},
     {failures, Perf#worker_perf.failures},
     {work_time, Perf#worker_perf.work_time + AddWork},
     {idle_time, Perf#worker_perf.idle_time + AddIdle}].

%% @doc Handle the fold request to start handoff.  Immediately ask all
%%      `waiting' workers to archive, and note that others should
%%      archive as they finish their current inputs.
-spec handoff_cmd_internal(term(), sender(), state()) ->
         {noreply, state()}.
handoff_cmd_internal(?FOLD_REQ{foldfun=Fold, acc0=Acc}, Sender,
              #state{workers=Workers}=State) ->
    {Ready, NotReady} = lists:partition(
                          fun(#worker{state={waiting,_}}) -> true;
                             (_)                          -> false
                          end,
                          Workers),
    %% ask waiting workers to produce archives
    Archiving = [ begin
                      send_archive(W),
                      W#worker{state={working, archive}}
                  end || W <- Ready ],
    {noreply, State#state{workers=NotReady, workers_archiving=Archiving,
                          handoff=#handoff{fold=Fold,
                                           acc=Acc,
                                           sender=Sender}}}.

%% @doc The vnode is in handoff, and a worker requested its next
%%      input.  Instead of giving it the next input, ask it to
%%      archive, so it can be sent to the handoff partner.
-spec archive_fitting(riak_pipe:fitting(), state()) -> state().
archive_fitting(F, State) ->
    case worker_by_fitting(F, State) of
        {ok, W} ->
            send_archive(W),
            CleanState = remove_worker(W, State),
            CleanState#state{
              workers_archiving=[W#worker{state={working, archive}}
                                 |State#state.workers_archiving]};
        none ->
            %% the requested queue isn't here; the fitting may have
            %% died, and this next_input request passed its kill in
            %% flight - just ignore
            State
    end.

%% @doc A worker finished archiving, and sent the archive back to the
%%      vnode.  Evaluate the handoff fold function, and remove the
%%      worker from the vnode's state.
%%
%%      If there are no more workers to archive, reply to the handoff
%%      requester with the accumulated result.
-spec archive_internal(#cmd_archive{}, state()) -> {noreply, state()}.
archive_internal(#cmd_archive{fitting=F, archive=A},
                 #state{handoff=Handoff,
                        workers=Workers,
                        workers_archiving=Archiving}=State) ->
    {value, Worker, NewArchiving} =
        lists:keytake(F, #worker.fitting, Archiving),
    HandoffVal = {Worker#worker.q, Worker#worker.blocking, A},
    NewAcc = (Handoff#handoff.fold)(F, HandoffVal, Handoff#handoff.acc),
    case {Workers, NewArchiving} of
        {[], []} ->
            %% handoff is done!
            riak_core_vnode:reply(Handoff#handoff.sender, NewAcc);
        _ ->
            %% still chugging
            ok
    end,
    {noreply, State#state{workers_archiving=NewArchiving,
                          handoff=Handoff#handoff{acc=NewAcc}}}.
