-module(ppg2).

-export([join/2, leave/2]).
-export([get_members/1]).
-export([get_closest_pid/1]).
-export([start/1,start_link/1, init/1,handle_call/3,handle_cast/2,handle_info/2, code_change/3,
         terminate/2]).

-behavior(gen_server).
%%% As of R13B03 monitors are used instead of links.

%%%
%%% Exported functions
%%%

-type name() :: any().

-spec start_link(Name :: name()) -> {'ok', pid()} | {'error', any()}.

start_link(Name) ->
    gen_server:start_link({local, Name}, ?MODULE, [], []).

-spec start(Name :: name()) -> {'ok', pid()} | {'error', any()}.

start(Name) ->
    gen_server:start({local, Name}, ?MODULE, [], []).


join(Name, Pid) when is_pid(Pid) ->
    gen_server:call(Name, {join, Pid})
.

-spec leave(Name, Pid :: pid()) -> 'ok' | {'error', {'no_such_group', Name}}
      when Name :: name().

leave(Name, Pid) when is_pid(Pid) ->
    gen_server:call(Name, {leave, Pid})
.

-spec get_members(Name) -> [pid()] | {'error', {'no_such_group', Name}}
      when Name :: name().

get_members(Name) ->
    gen_server:call(Name, {get_members})
.


-spec get_closest_pid(Name) ->  pid() | {'error', Reason} when
      Name :: name(),
      Reason ::  {'no_process', Name} | {'no_such_group', Name}.

get_closest_pid(Name) ->
    gen_server:call(Name, {get_closest_pid, Name})
.


%%%
%%% Callback functions from gen_server
%%%

-record(state, {}).

-type state() :: #state{}.

-spec init(Arg :: []) -> {'ok', state()}.

init([]) ->
        {ok, {[], dict:new()}}
.

-spec handle_call(Call :: {'create', Name}
                        | {'delete', Name}
                        | {'join', Name, Pid :: pid()}
                        | {'leave', Name, Pid :: pid()},
                  From :: {pid(),Tag :: any()},
                  State :: state()) -> {'reply', 'ok', state()}
      when Name :: name().

handle_call({join, Pid}, _From, S={Set, Map}) ->
    S2 = case lists:member(Pid, Set) of 
        true ->
            S; 
         false ->
            Ref = erlang:monitor(process, Pid),
            {[Pid|Set], dict:store(Pid, Ref, Map)}
    end, 
    {reply, ok, S2};

handle_call({leave, Pid}, _From, _S={Set, Map}) ->
    try 
        begin 
            Ref = dict:fetch(Pid, Map),
            erlang:demonitor(Ref)
        end
    catch 
        _:_ -> ok
    end,
    Set2 = lists:delete(Pid, Set),
    Map2 = dict:erase(Pid, Map),    
    {reply, ok, {Set2, Map2}};

handle_call({get_members}, _From, S={Set, _}) ->
    {reply, {ok, Set}, S};

handle_call({get_closest_pid, Name}, _From, S={Set, _}) ->
    R = case Set of 
        [] -> {error, {no_process, Name}};
        Members ->
            {_,_,X} = erlang:now(),
            lists:nth((X rem length(Members))+1, Members)
    end,
    {reply, R, S};

handle_call(Request, From, S) ->
    error_logger:warning_msg("The ppg2 server received an unexpected message:\n"
                             "handle_call(~p, ~p, _)\n", 
                             [Request, From]),
    {noreply, S}.

-spec handle_cast(Cast :: {'exchange', node(), Names :: [[Name,...]]}
                        | {'del_member', Name, Pid :: pid()},
                  State :: state()) -> {'noreply', state()}
      when Name :: name().

handle_cast(_, S) ->
    %% Ignore {del_member, Name, Pid}.
    {noreply, S}.

-spec handle_info(Tuple :: tuple(), State :: state()) ->
    {'noreply', state()}.

handle_info({'DOWN', _MonitorRef, process, Pid, _Info}, _S={Set, Map}) ->
    Set2 = lists:delete(Pid, Set),
    Map2 = dict:erase(Pid, Map),    
    {noreply, {Set2, Map2}};

handle_info(_, S) ->
    {noreply, S}.

code_change(_OldVersion, Library, _Extra) -> {ok, Library}.

-spec terminate(Reason :: any(), State :: state()) -> 'ok'.

terminate(_Reason, _S) ->
ok.
