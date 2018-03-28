-module(riak_repl2_rtsource_conn_data_mgr_tests).
-author("nordine saadouni").

-include_lib("eunit/include/eunit.hrl").

-define(SOURCE_3, [node_c, node_b, node()]).
-define(SOURCE_5, [node_e, node_d, node_c, node_b, node()]).
-define(SOURCE_8, [node_h, node_g, node_f, node_e, node_d, node_c, node_b, node()]).

-define(ACTIVE_CONNS, dict:from_list
([
  {node(),[{{"127.0.0.1",5001},true}]},
  {node_b,[{{"127.0.0.1",5002},true}]},
  {node_c,[{{"127.0.0.1",5003},true}]},
  {node_d,[{{"127.0.0.1",5004},true}]},
  {node_e,[{{"127.0.0.1",5005},true}]}
])).

connection_switching_in_rebalancing_test_() ->
  {spawn,
    [
      {setup,
        fun() ->
          Apps = riak_repl_test_util:maybe_start_lager(),
          _MockRing = ets:new(mock_ring, [named_table, public]),
          %% ring_trans
          riak_core_ring_manager_start(),
          %% nodes
          riak_core_node_watcher_start(),
          %% get_active_nodes, get_realtime_connection_data
          riak_repl_ring_start(),
          {ok, _Pid} = riak_repl2_rtsource_conn_data_mgr:start_link(),
          riak_repl2_rtsource_conn_data_mgr:set_leader(node(), pid),

          % set environemntal variable (node watcher polling time)

          Apps
        end,
        fun(StartedApps) ->
          catch(meck:unload(riak_core_ring_manager)),
          catch(meck:unload(riak_core_node_watcher)),
          catch(meck:unload(riak_repl_ring)),
          process_flag(trap_exit, false),
          riak_repl_test_util:stop_apps(StartedApps),
          ok
        end,

        fun(_) ->
          [
%%            {"initialization",
%%              fun() ->
%%                timer:sleep(1500),
%%                ct:pal("mock ring: ~p", [ets:tab2list(mock_ring)]),
%%                ct:pal("active nodes ~p", [riak_repl2_rtsource_conn_data_mgr:read(active_nodes)])
%%              end
%%            }



          ]
        end
      }
      ]
  }.


%%------------------------
%% Start up functions
%%------------------------
%% list all the possibilities here and then push them to an ets table!
%% ets table = mock_ring
riak_core_ring_manager_start() ->

  Fun1 = fun riak_repl_ring:overwrite_realtime_connection_data/2,
  Fun2 = fun riak_repl_ring:overwrite_active_nodes/2,
  Fun3 = fun riak_repl_ring:overwrite_active_nodes_and_realtime_connection_data/2,
  catch(meck:unload(riak_core_ring_manager)),
  meck:new(riak_core_ring_manager, [passthrough]),
  meck:expect(riak_core_ring_manager, ring_trans,
    fun(X,Y) ->
      case {X, Y} of
        {Fun1, NewConnections} ->
          ets:insert(mock_ring, {realtime_connections, NewConnections}),
          ok;
        {Fun2, NewActiveNodes} ->
          ets:insert(mock_ring, {active_nodes,NewActiveNodes}),
          ok;
        {Fun3, {NewConnections, NewActiveNodes}} ->
          ets:insert(mock_ring, {realtime_connections, NewConnections}),
          ets:insert(mock_ring, {active_nodes,NewActiveNodes}),
          ok
      end
    end
  ).

riak_core_node_watcher_start() ->
  catch(meck:unload(riak_core_node_watcher)),
  meck:new(riak_core_node_watcher, [passthrough]),
  dynamic_node_watcher(?SOURCE_5).

riak_repl_ring_start() ->
  catch(meck:unload(riak_repl_ring)),
  meck:new(riak_repl_ring, [passthrough]),
  meck:expect(riak_repl_ring, get_active_nodes,
    fun() ->
      {active_nodes, ActiveNodes} = ets:lookup(mock_ring, active_nodes),
      ActiveNodes
    end
  ),
  meck:expect(riak_repl_ring, get_realtime_connection_data,
    fun() ->
      {realtime_connections, RealtimeConnections} = ets:lookup(mock_ring, realtime_connections),
      RealtimeConnections
    end
  ).


% ----------------------------------------- %
%         Dynamic Meck Expects              %
% ----------------------------------------- %
dynamic_node_watcher(SourceNodes) ->
  meck:expect(riak_core_node_watcher, nodes,
    fun(_) ->
      SourceNodes
    end
  ).