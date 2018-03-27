-module(riak_repl2_rtsource_conn_mgr_tests).
-author("nordines aadouni").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-define(SOURCE_3, [node_c, node_b, node()]).
-define(SOURCE_5, [node_e, node_d, node_c, node_b, node()]).
-define(SOURCE_8, [node_h, node_g, node_f, node_e, node_d, node_c, node_b, node()]).
-define(SINK_3, [{"127.0.0.1",5001}, {"127.0.0.1",5002}, {"127.0.0.1",5003}]).
-define(SINK_5, [{"127.0.0.1",5001}, {"127.0.0.1",5002}, {"127.0.0.1",5003}, {"127.0.0.1",5004}, {"127.0.0.1",5005}]).
-define(SINK_8, [{"127.0.0.1",5001}, {"127.0.0.1",5002}, {"127.0.0.1",5003}, {"127.0.0.1",5004}, {"127.0.0.1",5005}, {"127.0.0.1",5006}, {"127.0.0.1",5007}, {"127.0.0.1",5008}]).

-define(SINK_3_EDGE_CASE, [{"127.0.0.1",5004}, {"127.0.0.1",5002}, {"127.0.0.1",5003}]).

-define(ACTIVE_CONNS, dict:from_list
([
  {node(),[{{"127.0.0.1",5001},true}]},
  {node_b,[{{"127.0.0.1",5002},true}]},
  {node_c,[{{"127.0.0.1",5003},true}]},
  {node_d,[{{"127.0.0.1",5004},true}]},
  {node_e,[{{"127.0.0.1",5005},true}]}
])).

-export([loop_connect/2]).


connection_switching_in_rebalancing_test_() ->
  {spawn,
    [
      {setup,
        fun() ->
          Apps = riak_repl_test_util:maybe_start_lager(),
          % controlling_process
          gen_tcp_controlling_process(),
          % sync_connect
          riak_core_connection_start(),
          % connect, disconnect
          riak_core_connection_mgr_start(),
          % register
          riak_repl2_rtq_start(),
          % read(ac & realtime) , write(realtime), delete(realtime)
          riak_repl2_rtsource_conn_data_mgr_start(),
          % get_unshuffled_ipaddrs, get_ipaddrs_of_cluster
          riak_core_cluster_mgr_start(),
          % start_link, stop, connected, status, get_helper_pid
          riak_repl2_rtsource_conn_start(),
          % stop_pulling
          riak_repl2_rtsource_helper(),

          Apps
        end,

        fun(StartedApps) ->
          catch(meck:unload(riak_core_connection)),
          catch(meck:unload(riak_core_connection_mgr)),
          catch(meck:unload(riak_repl2_rtq)),
          catch(meck:unload(riak_repl2_rtsource_conn_data_mgr)),
          catch(meck:unload(riak_core_cluster_mgr)),
          catch(meck:unload(riak_repl2_rtsource_conn)),
          catch(meck:unload(riak_repl2_rtsource_helper)),
          catch(meck:unload(gen_tcp)),
          process_flag(trap_exit, false),
          riak_repl_test_util:stop_apps(StartedApps),
          ok
        end,

        fun(_) ->
          [

            {"Initialization",
              fun() ->

                %% Table Setup
                B = ets:new(before_, [named_table, public]),
                A = ets:new(after_, [named_table, public]),

                PidTable = ets:new(pid_table, [named_table, public]),
                {ok, Pid} = riak_repl2_rtsource_remote_conn_sup:start_link("ClusterName"),
                ConnMgrPid = riak_repl2_rtsource_conn_sup:first_or_empty
                (
                  [ P || {_, P, _, [riak_repl2_rtsource_conn_mgr]} <- supervisor:which_children(Pid), is_pid(P)]
                ),

                ets:insert(PidTable, {rtsource_remote_conn_sup, Pid}),
                ets:insert(PidTable, {rtsource_conn_mgr, ConnMgrPid}),
                timer:sleep(1000),
                clean(A,B, false)
              end
            },

            {"Test 1 ",
              fun() ->

                [{rtsource_conn_mgr, ConnMgrPid}] = ets:lookup(pid_table, rtsource_conn_mgr),

                %% Table Setup
                B = ets:new(before_, [named_table, public]),
                A = ets:new(after_, [named_table, public]),


                %% Alter Connections and Rebalance
                change_variables(?SOURCE_3, ?SINK_3, ?ACTIVE_CONNS),
                erlang:send(ConnMgrPid, rebalance_now),
                timer:sleep(1000),

                %% Testing the transitions of connections for our node
                F = fun({X,_},{Y,_}) -> X =< Y end,
                Before = lists:sort(F, ets:tab2list(B)),
                After = lists:sort(F, ets:tab2list(A)),

                %% length should be 0 (as we do not re-establish any connection)
                ?assertEqual(0, length(Before)),
                ?assertEqual(0, length(After)),
                ?assertEqual([], Before),
                ?assertEqual([], After),
                clean(A,B, {true, ConnMgrPid})
              end
            },

            {"Test 2 ",
              fun() ->

                [{rtsource_conn_mgr, ConnMgrPid}] = ets:lookup(pid_table, rtsource_conn_mgr),

                %% Table Setup
                B = ets:new(before_, [named_table, public]),
                A = ets:new(after_, [named_table, public]),


                %% Alter Connections and Rebalance
                change_variables(?SOURCE_3, ?SINK_5, ?ACTIVE_CONNS),
                erlang:send(ConnMgrPid, rebalance_now),
                timer:sleep(1000),

                %% Testing the transitions of connections for our node
                F = fun({X,_},{Y,_}) -> X =< Y end,
                Before = lists:sort(F, ets:tab2list(B)),
                After = lists:sort(F, ets:tab2list(A)),


                %% Length shoould be 1
                ?assertEqual(1, length(Before)),
                ?assertEqual(1, length(After)),

                %% 1) Before -> 5001
                ?assertEqual([5001], get_port_list(lists:nth(1,Before))),
                %% 1) After -> 5001, 5004
                ?assertEqual([5001, 5004], get_port_list(lists:nth(1,After))),

                clean(A,B, {true, ConnMgrPid})
              end
            },

            {"Test 3 ",
              fun() ->

                [{rtsource_conn_mgr, ConnMgrPid}] = ets:lookup(pid_table, rtsource_conn_mgr),

                %% Table Setup
                B = ets:new(before_, [named_table, public]),
                A = ets:new(after_, [named_table, public]),


                %% Alter Connections and Rebalance
                change_variables(?SOURCE_3, ?SINK_8, ?ACTIVE_CONNS),
                erlang:send(ConnMgrPid, rebalance_now),
                timer:sleep(1000),

                %% Testing the transitions of connections for our node
                F = fun({X,_},{Y,_}) -> X =< Y end,
                Before = lists:sort(F, ets:tab2list(B)),
                After = lists:sort(F, ets:tab2list(A)),

                %% Length shoould be 2
                ?assertEqual(2, length(Before)),
                ?assertEqual(2, length(After)),

                %% 1) Before -> 5001
                ?assertEqual([5001], get_port_list(lists:nth(1,Before))),
                %% 1) After -> 5001, 5004
                ?assertEqual([5001, 5004], get_port_list(lists:nth(1,After))),

                %% 2) Before -> 5001
                ?assertEqual([5001, 5004], get_port_list(lists:nth(2,Before))),
                %% 2) After -> 5001, 5004
                ?assertEqual([5001, 5004, 5007], get_port_list(lists:nth(2 ,After))),

                clean(A,B, {true, ConnMgrPid})
              end
            },

            {"Test 4 ",
              fun() ->

                [{rtsource_conn_mgr, ConnMgrPid}] = ets:lookup(pid_table, rtsource_conn_mgr),

                %% Table Setup
                B = ets:new(before_, [named_table, public]),
                A = ets:new(after_, [named_table, public]),


                %% Alter Connections and Rebalance
                change_variables(?SOURCE_5, ?SINK_3, ?ACTIVE_CONNS),
                erlang:send(ConnMgrPid, rebalance_now),
                timer:sleep(1000),

                %% Testing the transitions of connections for our node
                F = fun({X,_},{Y,_}) -> X =< Y end,
                Before = lists:sort(F, ets:tab2list(B)),
                After = lists:sort(F, ets:tab2list(A)),

                %% length should be 0 (as we do not re-establish any connection)
                ?assertEqual(0, length(Before)),
                ?assertEqual(0, length(After)),
                ?assertEqual([], Before),
                ?assertEqual([], After),
                clean(A,B, {true, ConnMgrPid})
              end
            },

            {"Test 5 ",
              fun() ->

                [{rtsource_conn_mgr, ConnMgrPid}] = ets:lookup(pid_table, rtsource_conn_mgr),

                %% Table Setup
                B = ets:new(before_, [named_table, public]),
                A = ets:new(after_, [named_table, public]),


                %% Alter Connections and Rebalance
                change_variables(?SOURCE_5, ?SINK_5, ?ACTIVE_CONNS),
                erlang:send(ConnMgrPid, rebalance_now),
                timer:sleep(1000),

                %% Testing the transitions of connections for our node
                F = fun({X,_},{Y,_}) -> X =< Y end,
                Before = lists:sort(F, ets:tab2list(B)),
                After = lists:sort(F, ets:tab2list(A)),

                %% length should be 0 (as we do not re-establish any connection)
                ?assertEqual(0, length(Before)),
                ?assertEqual(0, length(After)),
                ?assertEqual([], Before),
                ?assertEqual([], After),
                clean(A,B, {true, ConnMgrPid})
              end
            },

            {"Test 6 ",
              fun() ->

                [{rtsource_conn_mgr, ConnMgrPid}] = ets:lookup(pid_table, rtsource_conn_mgr),

                %% Table Setup
                B = ets:new(before_, [named_table, public]),
                A = ets:new(after_, [named_table, public]),


                %% Alter Connections and Rebalance
                change_variables(?SOURCE_5, ?SINK_8, ?ACTIVE_CONNS),
                erlang:send(ConnMgrPid, rebalance_now),
                timer:sleep(1000),

                %% Testing the transitions of connections for our node
                F = fun({X,_},{Y,_}) -> X =< Y end,
                Before = lists:sort(F, ets:tab2list(B)),
                After = lists:sort(F, ets:tab2list(A)),

                %% Length shoould be 1
                ?assertEqual(1, length(Before)),
                ?assertEqual(1, length(After)),

                %% 1) Before -> 5001
                ?assertEqual([5001], get_port_list(lists:nth(1,Before))),
                %% 1) After -> 5001, 5004
                ?assertEqual([5001, 5006], get_port_list(lists:nth(1,After))),

                clean(A,B, {true, ConnMgrPid})
              end
            },

            {"Test 7 ",
              fun() ->

                [{rtsource_conn_mgr, ConnMgrPid}] = ets:lookup(pid_table, rtsource_conn_mgr),

                %% Table Setup
                B = ets:new(before_, [named_table, public]),
                A = ets:new(after_, [named_table, public]),


                %% Alter Connections and Rebalance
                change_variables(?SOURCE_8, ?SINK_3, ?ACTIVE_CONNS),
                erlang:send(ConnMgrPid, rebalance_now),
                timer:sleep(1000),

                %% Testing the transitions of connections for our node
                F = fun({X,_},{Y,_}) -> X =< Y end,
                Before = lists:sort(F, ets:tab2list(B)),
                After = lists:sort(F, ets:tab2list(A)),

                %% length should be 0 (as we do not re-establish any connection)
                ?assertEqual(0, length(Before)),
                ?assertEqual(0, length(After)),
                ?assertEqual([], Before),
                ?assertEqual([], After),
                clean(A,B, {true, ConnMgrPid})
              end
            },

            {"Test 8 ",
              fun() ->

                [{rtsource_conn_mgr, ConnMgrPid}] = ets:lookup(pid_table, rtsource_conn_mgr),

                %% Table Setup
                B = ets:new(before_, [named_table, public]),
                A = ets:new(after_, [named_table, public]),


                %% Alter Connections and Rebalance
                change_variables(?SOURCE_8, ?SINK_5, ?ACTIVE_CONNS),
                erlang:send(ConnMgrPid, rebalance_now),
                timer:sleep(1000),

                %% Testing the transitions of connections for our node
                F = fun({X,_},{Y,_}) -> X =< Y end,
                Before = lists:sort(F, ets:tab2list(B)),
                After = lists:sort(F, ets:tab2list(A)),

                %% length should be 0 (as we do not re-establish any connection)
                ?assertEqual(0, length(Before)),
                ?assertEqual(0, length(After)),
                ?assertEqual([], Before),
                ?assertEqual([], After),
                clean(A,B, {true, ConnMgrPid})
              end
            },

            {"Test 9 ",
              fun() ->

                [{rtsource_conn_mgr, ConnMgrPid}] = ets:lookup(pid_table, rtsource_conn_mgr),

                %% Table Setup
                B = ets:new(before_, [named_table, public]),
                A = ets:new(after_, [named_table, public]),


                %% Alter Connections and Rebalance
                change_variables(?SOURCE_8, ?SINK_8, ?ACTIVE_CONNS),
                erlang:send(ConnMgrPid, rebalance_now),
                timer:sleep(1000),

                %% Testing the transitions of connections for our node
                F = fun({X,_},{Y,_}) -> X =< Y end,
                Before = lists:sort(F, ets:tab2list(B)),
                After = lists:sort(F, ets:tab2list(A)),

                %% length should be 0 (as we do not re-establish any connection)
                ?assertEqual(0, length(Before)),
                ?assertEqual(0, length(After)),
                ?assertEqual([], Before),
                ?assertEqual([], After),
                clean(A,B, {true, ConnMgrPid})
              end
            },

            {"Test 10 (edge case) ",
              fun() ->

                [{rtsource_conn_mgr, ConnMgrPid}] = ets:lookup(pid_table, rtsource_conn_mgr),

                %% Table Setup
                B = ets:new(before_, [named_table, public]),
                A = ets:new(after_, [named_table, public]),


                %% Alter Connections and Rebalance
                change_variables(?SOURCE_5, ?SINK_3_EDGE_CASE, ?ACTIVE_CONNS),
                erlang:send(ConnMgrPid, rebalance_now),
                timer:sleep(1000),

                %% Testing the transitions of connections for our node
                F = fun({X,_},{Y,_}) -> X =< Y end,
                Before = lists:sort(F, ets:tab2list(B)),
                After = lists:sort(F, ets:tab2list(A)),

                %% Length shoould be 1
                ?assertEqual(1, length(Before)),
                ?assertEqual(1, length(After)),

                %% 1) Before -> 5001
                ?assertEqual([5001], get_port_list(lists:nth(1,Before))),
                %% 1) After -> 5001, 5004
                ?assertEqual([5004], get_port_list(lists:nth(1,After))),

                clean(A,B, {true, ConnMgrPid})
              end
            }

          ]
        end
      }
    ]
  }.


%%------------------------
%% Start up functions
%%------------------------
riak_core_connection_start() ->
  catch(meck:unload(riak_core_connection)),
  meck:new(riak_core_connection, [passthrough]),
  meck:expect(riak_core_connection, sync_connect,
    fun(_Addr, _Primary, _ClientSpec) ->
      ok
    end).

riak_core_connection_mgr_start() ->
  catch(meck:unload(riak_core_connection_mgr)),
  meck:new(riak_core_connection_mgr, [passthrough]),
  meck:expect(riak_core_connection_mgr, disconnect,
    fun(_Target) ->
      ok
    end),
  dynamic_connection_mgr_connect(?SINK_5).

riak_repl2_rtq_start() ->
  catch(meck:unload(riak_repl2_rtq)),
  meck:new(riak_repl2_rtq, [passthrough]),
  meck:expect(riak_repl2_rtq, register,
    fun(_Remote) ->
      {ok, 2}
    end).

riak_repl2_rtsource_conn_data_mgr_start() ->
  catch(meck:unload(riak_repl2_rtsource_conn_data_mgr)),
  meck:new(riak_repl2_rtsource_conn_data_mgr, [passthrough]),
  meck:expect(riak_repl2_rtsource_conn_data_mgr, write,
    fun(realtime_connections, _Remote, _Node, _IPPort, _Primary) ->
      ok
    end),
  meck:expect(riak_repl2_rtsource_conn_data_mgr, delete,
    fun(realtime_connections, _Remote, _Node, _IPPort, _Primary) ->
      ok
    end),
  dynamic_data_mgr_read_active_nodes(?SOURCE_5),
  dynamic_data_mgr_read_realtime_connections(dict:new()).


riak_core_cluster_mgr_start() ->
  catch(meck:unload(riak_core_cluster_mgr)),
  meck:new(riak_core_cluster_mgr, [passthrough]),
  dynamic_core_cluster_mgr_get_unshuffled_ips(?SINK_5),
  dynamic_core_cluster_mgr_get_ipaddrs(?SINK_5).

riak_repl2_rtsource_conn_start() ->
  catch(meck:unload(riak_repl2_rtsource_conn)),
  meck:new(riak_repl2_rtsource_conn, [passthrough]),
  meck:expect(riak_repl2_rtsource_conn, start_link,
    fun(_Remote, ConnMgrPid) ->
      {ok, ConnMgrPid}
    end
  ),
  meck:expect(riak_repl2_rtsource_conn, stop,
    fun(_Pid) ->
      ok
    end
  ),
  meck:expect(riak_repl2_rtsource_conn, connected,
    fun(_Socket, _Transport, _IPPort, _Proto, _RtSourcePid, _Props, _Primary) ->
      ok
    end
  ),
  meck:expect(riak_repl2_rtsource_conn, get_helper_pid,
    fun(_RtSourcePid) ->
      pid
    end
  ),
  meck:expect(riak_repl2_rtsource_conn, status,
    fun(_Pid) ->
      []
    end
  ),
  meck:expect(riak_repl2_rtsource_conn, status,
    fun(_Pid, _Timeout) ->
      []
    end
  ).

riak_repl2_rtsource_helper() ->
  catch(meck:unload(riak_repl2_rtsource_helper)),
  meck:new(riak_repl2_rtsource_helper, [passthrough]),
  meck:expect(riak_repl2_rtsource_helper, stop_pulling,
    fun(_Pid) ->
      ok
    end
  ).

gen_tcp_controlling_process() ->
  catch(meck:unload(gen_tcp)),
  meck:new(gen_tcp, [unstick, passthrough]),
  meck:expect(gen_tcp, controlling_process,
    fun(_Socket, _Pid) ->
      ok
    end
    ).






% ----------------------------------------- %
%         Dynamic Meck Expects              %
% ----------------------------------------- %

%%meck:expect(riak_core_connection_mgr, connect,
%%fun({rt_repl, _Remote}, ClientSpec, {use_only, Addrs}) ->
%%end),

dynamic_connection_mgr_connect(SinkNodes) ->
  meck:expect(riak_core_connection_mgr, connect,
    fun({rt_repl, _Remote}, ClientSpec, Type) ->
      {_, {_, _, Pid}} = ClientSpec,
      case Type of
        multi_connection ->
          {ok, {Primary, _Secondary}} = riak_core_cluster_mgr:get_my_remote_ip_list("ClusterName", SinkNodes, split),
          spawn_link(?MODULE, loop_connect, [Primary, Pid]),
          {ok, ref};
        {use_only, Addrs} ->
          spawn_link(?MODULE, loop_connect, [Addrs, Pid]),
          {ok, ref}
      end
    end).

dynamic_data_mgr_read_active_nodes(ActiveNodes) ->
  meck:expect(riak_repl2_rtsource_conn_data_mgr, read,
    fun(active_nodes) ->
      ActiveNodes
    end).

dynamic_data_mgr_read_realtime_connections(RealtimeConnections) ->
  meck:expect(riak_repl2_rtsource_conn_data_mgr, read,
    fun(realtime_connections, _Remote) ->
      RealtimeConnections
    end).

dynamic_core_cluster_mgr_get_unshuffled_ips(SinkNodes) ->
  meck:expect(riak_core_cluster_mgr, get_unshuffled_ipaddrs_of_cluster,
    fun(_Remote) ->
      SinkNodes
    end).

dynamic_core_cluster_mgr_get_ipaddrs(SinkNodes) ->
  meck:expect(riak_core_cluster_mgr, get_ipaddrs_of_cluster,
    fun(_Remote, split) ->
      riak_core_cluster_mgr:get_my_remote_ip_list("ClusterName", SinkNodes, split)
    end).

change_variables(SourceNodes, SinkNodes, RealtimeConnections) ->
  dynamic_data_mgr_read_active_nodes(SourceNodes),
  dynamic_core_cluster_mgr_get_unshuffled_ips(SinkNodes),
  dynamic_core_cluster_mgr_get_ipaddrs(SinkNodes),
  dynamic_data_mgr_read_realtime_connections(RealtimeConnections).

% -------------------------------------------- %
%           Auxiliary Functions                %
% -------------------------------------------- %

loop_connect(List, Pid) ->
  timer:sleep(100),
  loop_connect2(List, Pid).

loop_connect2([], _) ->
  ok;
loop_connect2([{IPPort, Primary} | Rest], Pid)->
  case Primary of
    true ->
      Key = os:timestamp(),
      ets:insert(before_, {Key, riak_repl2_rtsource_conn_mgr:get_endpoints(Pid)}),
      riak_repl2_rtsource_conn_mgr:connected(undefined, gen_tcp, IPPort, undefined, Pid, undefined, Primary),
      ets:insert(after_, {Key, riak_repl2_rtsource_conn_mgr:get_endpoints(Pid)});
    false ->
      ok
  end,
  loop_connect2(Rest, Pid).


clean(A, B, Revert) ->
  case Revert of
    {true, Pid} ->
      revert_state(Pid);
    false ->
      ok
  end,
  ets:delete(A),
  ets:delete(B).

revert_state(ConnMgrPid) ->
  change_variables(?SOURCE_5, ?SINK_5, dict:new()),
  erlang:send(ConnMgrPid, rebalance_now),
  timer:sleep(1000).


get_port_list(X) ->
  {_TimeStamp, ListOfConnections} = X,
  retrieve_ports(ListOfConnections, []).

retrieve_ports([], Saved) ->
  Saved;
retrieve_ports([{{{_,Port},_},_}|Rest], Saved) ->
  retrieve_ports(Rest, Saved++[Port]).




-endif.

