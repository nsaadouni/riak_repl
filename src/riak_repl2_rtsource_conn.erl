%% Riak EnterpriseDS
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.

%% @doc Realtime replication source connection module
%%
%% Works in tandem with rtsource_helper.  The helper interacts with
%% the RTQ to send queued traffic over the socket.  This rtsource_conn
%% process accepts the remote Acks and clears the RTQ.
%%
%% If both sides support heartbeat message, it is sent from the RT source
%% every `{riak_repl, rt_heartbeat_interval}' which default to 15s.  If
%% a response is not received in {riak_repl, rt_heartbeat_timeout}, also
%% default to 15s then the source connection exits and will be re-established
%% by the supervisor.
%%
%% 1. On startup/interval timer - `rtsource_conn' casts to `rtsource_helper'
%%    to send over the socket.  If TCP buffer is full or `rtsource_helper'
%%    is otherwise hung the `rtsource_conn' process will still continue.
%%    `rtsource_conn' sets up a heartbeat timeout.
%%
%% 2. At rtsink, on receipt of a heartbeat message it sends back
%%    a heartbeat message and stores the timestamp it last received one.
%%    The rtsink does not worry about detecting broken connections
%%    as new ones can be established harmlessly.  Keep it simple.
%%
%% 3. If rtsource receives the heartbeat back, it cancels the timer
%%    and updates the hearbeat round trip time (`hb_rtt') then sets
%%    a new heartbeat_interval timer.
%%
%%    If the heartbeat_timeout fires, the rtsource connection terminates.
%%    The `rtsource_helper:stop' call is now wrapped in a timeout in
%%    case it is hung so we don't get nasty messages about `rtsource_conn'
%%    crashing when it's the helper that is causing the problems.
-module(riak_repl2_rtsource_conn).

-behaviour(gen_server).
-include("riak_repl.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-export([riak_core_connection_mgr_connect/2]).
-endif.

%% API
-export([start_link/2,
         stop/2,
         get_helper_pid/1,
         status/1, status/2,
         get_address/1,
         get_socketname_primary/1,
         connected/7,
         legacy_status/1, legacy_status/2]).


%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(DEFAULT_HBINTERVAL, 15).
-define(DEFAULT_HBTIMEOUT, 15).

-define(TCP_OPTIONS,  [{keepalive, true},
                       {nodelay, true},
                       {packet, 0},
                       {active, false}]).

%% nodes running 1.3.1 have a bug in the service_mgr module.
%% this bug prevents it from being able to negotiate a version list longer
%% than 2. Until we no longer support communicating with that version,
%% we need to artifically truncate the version list.
%% TODO: expand version list or remove comment when we no
%% longer support 1.3.1
%% prefered version list: [{2,0}, {1,5}, {1,1}, {1,0}]


-define(CLIENT_SPEC, {{realtime,[{3,0}, {2,0}, {1,5}]},
                      {?TCP_OPTIONS, ?MODULE, self()}}).

-record(state, {remote,    % remote name
                address,   % {IP, Port}
                primary,
                conn_mgr_pid,
                transport, % transport module
                socket,    % socket to use with transport
                peername,  % cached when socket becomes active
                proto,     % protocol version negotiated
                ver,       % wire format negotiated
                helper_pid,% riak_repl2_rtsource_helper pid
                hb_interval,% seconds to send new heartbeat after last
                hb_interval_tref,
                hb_timeout,% seconds to wait for heartbeat after send
                hb_timeout_tref,% heartbeat timeout timer reference
                hb_sent_q,   % queue of heartbeats now() that were sent
                hb_rtt,    % RTT in milliseconds for last completed heartbeat
                cont = <<>>}). % continuation from previous TCP buffer

%% API - start trying to send realtime repl to remote site
start_link(Remote, ConnMgrPid) ->
    gen_server:start_link(?MODULE, [Remote, ConnMgrPid], []).

stop(Pid, do_not_remove_conns) ->
    gen_server:call(Pid, {stop, do_not_remove_conns}, ?LONG_TIMEOUT);
stop(Pid, remove_conns) ->
  gen_server:call(Pid, {stop, remove_conns}, ?LONG_TIMEOUT).

status(Pid) ->
    status(Pid, infinity).

status(Pid, Timeout) ->
    gen_server:call(Pid, status, Timeout).

%% legacy status -- look like a riak_repl_tcp_server
legacy_status(Pid) ->
    legacy_status(Pid, infinity).

legacy_status(Pid, Timeout) ->
    gen_server:call(Pid, legacy_status, Timeout).

connected(Socket, Transport, IPPort, Proto, RtSourcePid, _Props, Primary) ->
  Transport:controlling_process(Socket, RtSourcePid),
  Transport:setopts(Socket, [{active, true}]),
  try
    gen_server:call(RtSourcePid,
      {connected, Socket, Transport, IPPort, Proto, Primary},
      ?LONG_TIMEOUT)
  catch
    _:Reason ->
      lager:warning("Unable to contact RT source connection process (~p). Killing it to force reconnect.",
        [RtSourcePid]),
      exit(RtSourcePid, {unable_to_contact, Reason}),
      ok
  end.

get_helper_pid(RtSourcePid) ->
  gen_server:call(RtSourcePid, get_helper_pid).

get_address(Pid) ->
  gen_server:call(Pid, address, ?LONG_TIMEOUT).

get_socketname_primary(Pid) ->
  gen_server:call(Pid, get_socketname_primary).

% ======================================================================================================================

%% gen_server callbacks

%% Initialize
init([Remote, ConnMgrPid]) ->
  {ok, #state{remote = Remote, conn_mgr_pid = ConnMgrPid}}.

handle_call({stop, do_not_remove_conns}, _From, State) ->
    {stop, do_not_remove_conns, ok, State};
handle_call({stop, remove_conns}, _From, State) ->
  {stop, remove_conns, ok, State};
handle_call(address, _From, State = #state{address=A, primary=P}) ->
    {reply, {A,P}, State};
handle_call(get_socketname_primary, _From, State=#state{socket = S, primary = P}) ->
  {ok, Peername} = inet:sockname(S),
  {reply, {Peername, P}, State};
handle_call(status, _From, State =
                #state{remote = R, address = _A, transport = T, socket = S,
                       helper_pid = H,
                       hb_interval = HBInterval, hb_rtt = HBRTT}) ->
    Props = case T of
                undefined ->
                    [{connected, false}];
                _ ->
                    HBStats = case HBInterval of
                                  undefined ->
                                      [];
                                  _ ->
                                      [{hb_rtt, HBRTT}]
                              end,
                    SocketStats = riak_core_tcp_mon:socket_status(S),

                    [{connected, true},
                     %%{address, riak_repl_util:format_ip_and_port(A)},
                     {transport, T},
                     %%{socket_raw, S},
                     {socket,
                      riak_core_tcp_mon:format_socket_stats(SocketStats, [])},
                     %%{peername, peername(State)},
                     {helper_pid, riak_repl_util:safe_pid_to_list(H)}] ++
                        HBStats
            end,
    HelperProps = case H of
                      undefined ->
                          [];
                      _ ->
                          try
                              Timeout = app_helper:get_env(
                                          riak_repl, status_helper_timeout,
                                          app_helper:get_env(riak_repl, status_timeout, 5000) - 1000),
                              riak_repl2_rtsource_helper:status(H, Timeout)
                          catch
                              _:{timeout, _} ->
                                  [{helper, timeout}]
                          end
                  end,
    FormattedPid = riak_repl_util:safe_pid_to_list(self()),
    Status = [{sink, R}, {pid, FormattedPid}] ++ Props ++ HelperProps,
    {reply, Status, State};
handle_call(legacy_status, _From, State = #state{remote = Remote}) ->
    SocketStats = riak_core_tcp_mon:socket_status(State#state.socket),
    Socket = riak_core_tcp_mon:format_socket_stats(SocketStats, []),
    RTQStats = [{realtime_queue_stats, riak_repl2_rtq:status()}],
    Status =
        [{node, node()},
         {site, Remote},
         {strategy, realtime},
         {socket, Socket}] ++ RTQStats,
    {reply, {status, Status}, State};
handle_call({connected, Socket, Transport, EndPoint, Proto, Primary}, _From,
    State = #state{remote = Remote}) ->
  %% Check the socket is valid, may have been an error
  %% before turning it active (e.g. handoff of riak_core_service_mgr to handler
  case Transport:send(Socket, <<>>) of
    ok ->
      Ver = riak_repl_util:deduce_wire_version_from_proto(Proto),
      lager:debug("RT source connection negotiated ~p wire format from proto ~p", [Ver, Proto]),
      {_, ClientVer, _} = Proto,
      {ok, HelperPid} = riak_repl2_rtsource_helper:start_link(Remote, Transport, Socket, ClientVer),
      SocketTag = riak_repl_util:generate_socket_tag("rt_source", Transport, Socket),
      lager:debug("Keeping stats for " ++ SocketTag),
      riak_core_tcp_mon:monitor(Socket, {?TCP_MON_RT_APP, source,
        SocketTag}, Transport),
      State2 = State#state{transport = Transport,
        socket = Socket,
        address = EndPoint,
        proto = Proto,
        peername = peername(Transport, Socket),
        helper_pid = HelperPid,
        ver = Ver,
        primary = Primary},
      lager:info("Established realtime connection to site ~p address ~s",
        [Remote, peername(State2)]),

      case Proto of
        {realtime, _OurVer, {1, 0}} ->
          {reply, ok, State2};
        _ ->
          %% 1.1 and above, start with a heartbeat
          HBInterval = app_helper:get_env(riak_repl, rt_heartbeat_interval,
            ?DEFAULT_HBINTERVAL),
          HBTimeout = app_helper:get_env(riak_repl, rt_heartbeat_timeout,
            ?DEFAULT_HBTIMEOUT),
          State3 = State2#state{hb_interval = HBInterval,
            hb_timeout = HBTimeout,
            hb_sent_q = queue:new() },
          {reply, ok, send_heartbeat(State3)}
      end;
    ER ->
      {reply, ER, State}
  end;

handle_call(get_helper_pid, _From, State=#state{helper_pid = H}) ->
  {reply, H, State}.

handle_cast(_Request, State) ->
  {noreply, State}.


handle_info({Proto, _S, TcpBin}, State= #state{cont = Cont})
  when Proto == tcp; Proto == ssl ->
    recv(<<Cont/binary, TcpBin/binary>>, State);
handle_info({Closed, _S},
            State = #state{remote = Remote, cont = Cont})
  when Closed == tcp_closed; Closed == ssl_closed ->
    case size(Cont) of
        0 ->
            ok;
        NumBytes ->
            riak_repl_stats:rt_source_errors(),
            lager:warning("Realtime connection ~s to ~p closed with partial receive of ~b bytes\n",
                          [peername(State), Remote, NumBytes])
    end,
    {stop, remove_conns, State};
handle_info({Error, _S, Reason},
            State = #state{remote = Remote, cont = Cont})
  when Error == tcp_error; Error == ssl_error ->
    riak_repl_stats:rt_source_errors(),
    lager:warning("Realtime connection ~s to ~p network error ~p - ~b bytes pending\n",
                  [peername(State), Remote, Reason, size(Cont)]),
    {stop, remove_conns, State};

handle_info(send_heartbeat, State) ->
    {noreply, send_heartbeat(State)};
handle_info({heartbeat_timeout, HBSent}, State = #state{hb_sent_q = HBSentQ,
                                                        hb_timeout_tref = HBTRef,
                                                        hb_timeout = HBTimeout,
                                                        remote = Remote}) ->
    TimeSinceTimeout = timer:now_diff(now(), HBSent) div 1000,

    %% hb_timeout_tref is the authority of whether we should
    %% restart the conection on heartbeat timeout or not.
    case HBTRef of
        undefined ->
            lager:info("Realtime connection ~s to ~p heartbeat "
                       "time since timeout ~p",
                       [peername(State), Remote, TimeSinceTimeout]),
            {noreply, State};
        _ ->
            lager:warning("Realtime connection ~s to ~p heartbeat timeout "
                          "after ~p seconds\n",
                          [peername(State), Remote, HBTimeout]),
            lager:info("hb_sent_q_len after heartbeat_timeout: ~p", [queue:len(HBSentQ)]),
            {stop, remove_conns, State}
    end;

handle_info(Msg, State) ->
    lager:warning("Unhandled info:  ~p", [Msg]),
    {noreply, State}.

terminate(Reason, #state{helper_pid=HelperPid, conn_mgr_pid = C, address = A, primary = P}) ->
  case Reason of
    do_not_remove_conns ->
      ok;
    _ ->
      %% remove this connection from the manager
      riak_repl2_rtsource_conn_mgr:connection_closed(C, A, P)
  end,
  lager:info("rtsource conn terminated due to ~p", [Reason]),
  catch riak_repl2_rtsource_helper:stop(HelperPid),
  ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


cancel_timer(undefined) -> ok;
cancel_timer(TRef)      -> _ = erlang:cancel_timer(TRef).

recv(TcpBin, State = #state{remote = Name,
                            hb_sent_q = HBSentQ,
                            hb_timeout_tref = HBTRef}) ->
    %% hb_timeout_tref might be undefined if we have are getting
    %% acks/heartbeats back-to-back and we haven't sent a heartbeat yet.
    {realtime, {ProtoMajor, _}, {ProtoMajor, _}} = State#state.proto,
    case riak_repl2_rtframe:decode(TcpBin) of
        {ok, undefined, Cont} ->
            {noreply, State#state{cont = Cont}};
        {ok, {ack, Seq}, Cont} when ProtoMajor >= 2 ->
            %% TODO: report this better per-remote
            riak_repl_stats:objects_sent(),
            ok = riak_repl2_rtq:ack(Name, Seq),
            %% reset heartbeat timer, since we've seen activity from the peer
            case HBTRef of
                undefined ->
                    recv(Cont, State);
                _ ->
                    _ = cancel_timer(HBTRef),
                    recv(Cont, schedule_heartbeat(State#state{hb_timeout_tref=undefined}))
            end;
        {ok, {ack, Seq}, Cont} ->
            riak_repl2_rtsource_helper:v1_ack(State#state.helper_pid, Seq),
            %% reset heartbeat timer, since we've seen activity from the peer
            case HBTRef of
                undefined ->
                    recv(Cont, State);
                _ ->
                    _ = erlang:cancel_timer(HBTRef),
                    recv(Cont, schedule_heartbeat(State#state{hb_timeout_tref=undefined}))
            end;
        {ok, heartbeat, Cont} ->
            %% Compute last heartbeat roundtrip in msecs and
            %% reschedule next.
            {{value, HBSent}, HBSentQ2} = queue:out(HBSentQ),
            lager:debug("got heartbeat, hb_sent: ~w", [HBSent]),
            HBRTT = timer:now_diff(now(), HBSent) div 1000,
            _ = cancel_timer(HBTRef),
            State2 = State#state{hb_sent_q = HBSentQ2,
                                 hb_timeout_tref = undefined,
                                 hb_rtt = HBRTT},
            lager:debug("got heartbeat, hb_sent_q_len after heartbeat_recv: ~p", [queue:len(HBSentQ2)]),
            recv(Cont, schedule_heartbeat(State2))
    end.

peername(Transport, Socket) ->
    riak_repl_util:peername(Socket, Transport).

peername(#state{peername = P}) ->
    P.

%% Heartbeat is disabled, do nothing
send_heartbeat(State = #state{hb_interval = undefined}) ->
    State;
%% Heartbeat supported and enabled, tell helper to send the message,
%% and start the timeout.  Managing heartbeat from this process
%% will catch any bug that causes the helper process to hang as
%% well as connection issues - either way we want to re-establish.
send_heartbeat(State = #state{hb_timeout = HBTimeout,
                              hb_sent_q = SentQ,
                              helper_pid = HelperPid}) when is_integer(HBTimeout) ->

    % Using now as need a unique reference for this heartbeat
    % to spot late heartbeat timeout messages
    Now = now(),

    riak_repl2_rtsource_helper:send_heartbeat(HelperPid),
    TRef = erlang:send_after(timer:seconds(HBTimeout), self(), {heartbeat_timeout, Now}),
    State2 = State#state{hb_interval_tref = undefined, hb_timeout_tref = TRef,
                         hb_sent_q = queue:in(Now, SentQ)},
    lager:debug("hb_sent_q_len after sending heartbeat: ~p", [queue:len(SentQ)+1]),
    State2;
send_heartbeat(State) ->
    lager:warning("Heartbeat is misconfigured and is not a valid integer."),
    State.

%% Schedule the next heartbeat
schedule_heartbeat(State = #state{hb_interval_tref = undefined,
                                  hb_interval = HBInterval}) when is_integer(HBInterval) ->
    TRef = erlang:send_after(timer:seconds(HBInterval), self(), send_heartbeat),
    State#state{hb_interval_tref = TRef};

schedule_heartbeat(State = #state{hb_interval_tref = _TRef,
                                  hb_interval = HBInterval}) when is_integer(HBInterval) ->
    lager:debug("hb_interval_tref is not undefined when attempting to schedule new heartbeat."),
    State;

schedule_heartbeat(State) ->
    lager:warning("Heartbeat is misconfigured and is not a valid integer."),
    State.

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

riak_repl2_rtsource_conn_test_() ->
    {spawn, [{
        setup,
        fun setup/0,
        fun cleanup/1,
        {timeout, 120, fun cache_peername_test_case/0}
    }]}.

setup() ->
    % ?debugMsg("enter setup()"),
    % make sure there aren't leftovers around from prior tests
    sanitize(),
    % now set up the environment for this test
    process_flag(trap_exit, true),
    riak_repl_test_util:start_test_ring(),
    riak_repl_test_util:abstract_gen_tcp(),
    riak_repl_test_util:abstract_stats(),
    riak_repl_test_util:abstract_stateful(),
    % ?debugMsg("leave setup()"),
    ok.

cleanup(_Ctx) ->
    % ?debugFmt("enter cleanup(~p)", [_Ctx]),
    R = sanitize(),
    % ?debugFmt("leave cleanup(~p) -> ~p", [_Ctx, R]),
    R.

sanitize() ->
    % ?debugMsg("enter sanitize()"),
    rt_source_helpers:kill_fake_sink(),
    riak_repl_test_util:kill_and_wait([
        riak_repl2_rt,
        riak_repl2_rtq,
        riak_core_tcp_mon]),

    riak_repl_test_util:stop_test_ring(),

    riak_repl_test_util:maybe_unload_mecks([
        riak_core_service_mgr,
        riak_core_connection_mgr,
        gen_tcp]),
    meck:unload(),
    % ?debugMsg("leave sanitize()"),
    ok.

%% test for https://github.com/basho/riak_repl/issues/247
%% cache the peername so that when the local socket is closed
%% peername will still be around for logging
cache_peername_test_case() ->
    % ?debugMsg("enter cache_peername_test_case()"),
    {ok, _RTPid} = rt_source_helpers:start_rt(),
    {ok, _RTQPid} = rt_source_helpers:start_rtq(),
    {ok, _TCPMonPid} = rt_source_helpers:start_tcp_mon(),
    {ok, _FsPid, FsPort} = rt_source_helpers:init_fake_sink(),
    FsName = {"127.0.0.1", FsPort},
    ok = setup_connection_for_peername(FsName),

    ?assertEqual(ok, connect(FsName)).
    % ?debugMsg("leave cache_peername_test_case()").

%% Set up the test
setup_connection_for_peername(RemoteName) ->
    % ?debugFmt("enter setup_connection_for_peername(~p)", [RemoteName]),
    riak_repl_test_util:reset_meck(riak_core_connection_mgr, [no_link, passthrough]),
    meck:expect(riak_core_connection_mgr, connect,
                fun(_ServiceAndRemote, ClientSpec) ->
                        proc_lib:spawn_link(?MODULE, riak_core_connection_mgr_connect, [ClientSpec, RemoteName]),
                        {ok, make_ref()}
                end).
riak_core_connection_mgr_connect(ClientSpec, {RemoteHost, RemotePort} = RemoteName) ->
    Version = stateful:version(),
    {_Proto, {TcpOpts, Module, Pid}} = ClientSpec,
    {ok, Socket} = gen_tcp:connect(RemoteHost, RemotePort, [binary | TcpOpts]),

    ok = Module:connected(Socket, gen_tcp, RemoteName, Version, Pid, []),

    % simulate local socket problem
    inet:close(Socket),

    % get the State from the source connection.
    {status,Pid,_,[_,_,_,_,[_,_,{data,[{_,State}]}]]} = sys:get_status(Pid),
    % getting the peername from the socket should produce error string
    ?assertEqual("error:einval", peername(inet, Socket)),

    % while getting the peername from the State should produce the cached string
    % format the string we expect from peername(State) ...
    {ok, HostIP} = inet:getaddr(RemoteHost, inet),
    RemoteText = lists:flatten(io_lib:format("~B.~B.~B.~B:~B",
                    tuple_to_list(HostIP) ++ [RemotePort])),
    % ... and hook the function to check for it
    ?assertEqual(RemoteText, peername(State)).

%% Connect to the 'fake' sink
connect(RemoteName) ->
    % ?debugFmt("enter connect(~p)", [RemoteName]),

    stateful:set(version, {realtime, {1,0}, {1,0}}),
    stateful:set(remote, RemoteName),

    %% rtsource_conn now takes in the remote name and the conn_mgr pid
    %% As this test does not require the conn_mgr we pass self() as the conn_mgr pid in order to start
    %% rtsource_conn without failure.
    {ok, SourcePid} = riak_repl2_rtsource_conn:start_link(RemoteName, self()),

    {status, Status} = riak_repl2_rtsource_conn:legacy_status(SourcePid),
    RTQStats = proplists:get_value(realtime_queue_stats, Status),

    ?assertEqual([{percent_bytes_used, 0.0},
                  {bytes,0},
                  {max_bytes,104857600},
                  {consumers,[]},
                  {overload_drops,0}], RTQStats).

-endif.