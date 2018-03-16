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

-define(ACTIVE_CONNS, dict:from_list
([
  {node(),[{{"127.0.0.1",5001},true}]},
  {node_b,[{{"127.0.0.1",5002},true}]},
  {node_c,[{{"127.0.0.1",5003},true}]},
  {node_d,[{{"127.0.0.1",5004},true}]},
  {node_e,[{{"127.0.0.1",5005},true}]}
])).


connection_helper_logic_test_() ->
  {spawn,
    [
      {setup,
        fun() ->
          Apps = riak_repl_test_util:maybe_start_lager(),

          % sync_connect [done]
          riak_core_connection_start(),

          % connect, disconnect [done]
          riak_core_connection_mgr_start(?SINK_5),

          % register [done]
          riak_repl2_rtq_start(),

          % read(ac & realtime) , write(realtime), delete(realtime) [done]
          riak_repl2_rtsource_conn_data_mgr_start(?SOURCE_5, ?ACTIVE_CONNS),

          % get_unshuffled_ipaddrs, get_ipaddrs_of_cluster [done]
          riak_core_cluster_mgr_start(?SINK_5),

          % start_link, stop, connected, status, get_helper_pid
%%          riak_repl2_rtsource_conn_start(),

          % stop_pulling
%%          riak_repl2_rtsource_helper(),

          Apps
        end,

        fun(StartedApps) ->
          catch(meck:unload(riak_core_connection)),
          catch(meck:unload(riak_core_connection_mgr)),
          catch(meck:unload(riak_repl2_rtq)),
          catch(meck:unload(riak_repl2_rtsource_conn_data_mgr)),
          catch(meck:unload(riak_core_cluster_mgr)),
          process_flag(trap_exit, false),
          riak_repl_test_util:stop_apps(StartedApps),
          ok
        end,

        fun(_) ->
          [

            {"Start conn_mgr",
              fun() ->
                ok
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

riak_core_connection_mgr_start(SinkNodes) ->
  catch(meck:unload(riak_core_connection_mgr)),
  meck:new(riak_core_connection_mgr, [passthrough]),
  meck:expect(riak_core_connection_mgr, connect,
    fun({rt_repl, _Remote}, _ClientSpec, {use_only, Addrs}) ->
      loop_connect(Addrs)
    end),
  meck:expect(riak_core_connection_mgr, disconnect,
    fun(_Target) ->
      ok
    end),
  dynamic_connection_mgr_connect(SinkNodes).

riak_repl2_rtq_start() ->
  catch(meck:unload(riak_repl2_rtq)),
  meck:new(riak_repl2_rtq, [passthrough]),
  meck:expect(riak_repl2_rtq, register,
    fun(_Remote) ->
      {ok, 2}
    end).

riak_repl2_rtsource_conn_data_mgr_start(SourceNodes, RealtimeConnections) ->
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
  dynamic_data_mgr_read_active_nodes(SourceNodes),
  dynamic_data_mgr_read_realtime_connections(RealtimeConnections).


riak_core_cluster_mgr_start(SinkNodes) ->
  catch(meck:unload(riak_core_cluster_mgr)),
  meck:new(riak_core_cluster_mgr, [passthrough]),
  dynamic_core_cluster_mgr_get_unshuffled_ips(SinkNodes),
  dynamic_core_cluster_mgr_get_ipaddrs(SinkNodes).






% ----------------------------------------- %
%         Dynamic Meck Expects              %
% ----------------------------------------- %


dynamic_connection_mgr_connect(SinkNodes) ->
  meck:expect(riak_core_connection_mgr, connect,
    fun({rt_repl, _Remote}, _ClientSpec, multi_connection) ->
      {ok, {Primary, _Secondary}} = riak_core_cluster_mgr:get_my_remote_ip_list("ClusterName", SinkNodes, split),
      loop_connect(Primary)
    end).

dynamic_data_mgr_read_active_nodes(ActiveNodes) ->
  meck:expect(riak_repl2_rtsource_conn_data_mgr, read,
    fun(active_nodes) ->
      ActiveNodes
    end).

dynamic_data_mgr_read_realtime_connections(RealtimeConnections) ->
  meck:expect(riak_repl2_rtsource_conn_data_mgr, read,
    fun(_Remote) ->
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

%%change_sink_source_nodes(SinkNodes, SourceNodes) ->
%%  dynamic_data_mgr_read_active_nodes(SourceNodes),
%%  dynamic_core_cluster_mgr_get_unshuffled_ips(SinkNodes),
%%  dynamic_core_cluster_mgr_get_ipaddrs(SinkNodes).

% -------------------------------------------- %
%           Auxiliary Functions                %
% -------------------------------------------- %
loop_connect([]) ->
  {ok, ref};
loop_connect([{IPPort, Primary} | Rest])->
  riak_repl2_rtsource_conn_mgr:connected(undefined, undefined, IPPort,undefined, undefined, undefined, Primary),
  loop_connect(Rest).


-endif.

