-module(riak_repl2_rtsource_conn_data_mgr_tests).
-author("nordine saadouni").

-include_lib("eunit/include/eunit.hrl").


connection_switching_in_rebalancing_test_() ->
  {spawn,
    [
      {setup,
        fun() ->
          Apps = riak_repl_test_util:maybe_start_lager(),



          Apps
        end,

        fun(StartedApps) ->

          process_flag(trap_exit, false),
          riak_repl_test_util:stop_apps(StartedApps),
          ok
        end,

        fun(_) ->
          [

          ]
        end
      }
      ]
  }.