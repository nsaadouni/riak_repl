-module(riak_repl2_object_filter).
-behaviour(gen_server).

%% API
-export([
    start_link/0,
    start/0,
    stop/0,
    load/1
]).

%% gen_server callbacks
-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {}).

%%%===================================================================
%%% API
%%%==================================================================
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).
start()->
    gen_server:call(?MODULE, start).
stop()->
    gen_server:call(?MODULE, stop).
load(ConfigFilePath) ->
    gen_server:call(?MODULE, {load, ConfigFilePath}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================
init([]) ->
    {ok, #state{}}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
