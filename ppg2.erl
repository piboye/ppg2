-module(ppg2).

-export([join/2, leave/2]).
-export([get_members/1]).
-export([get_closest_pid/1]).
-export([start/0,start_link/0, init/1,handle_call/3,handle_cast/2,handle_info/2, code_change/3,
         terminate/2]).

-behavior(gen_server).
-define(SERVER, ?MODULE).
-define(PPG2TABLE, pp2_table).

-spec start() -> {'ok', pid()} | {'error', any()}.
start() ->
    gen_server:start({local, ?SERVER}, ?MODULE, [], []).

-type name() :: any().
-spec start_link() -> {'ok', pid()} | {'error', any()}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).


join(Name, Pid) when is_pid(Pid) ->
    gen_server:call(?SERVER, {join, Name, Pid})
.

-spec leave(Name, Pid :: pid()) -> 'ok' | {'error', {'no_such_group', Name}}
      when Name :: name().

leave(Name, Pid) when is_pid(Pid) ->
    gen_server:call(?SERVER, {leave, Name, Pid})
.

-spec get_members(Name) -> [pid()] | {'error', {'no_such_group', Name}}
      when Name :: name().

get_members(Name) ->
    gen_server:call(?SERVER, {get_members, Name})
.


-spec get_closest_pid(Name) ->  pid() | {'error', Reason} when
      Name :: name(),
      Reason ::  {'no_process', Name} | {'no_such_group', Name}.

get_closest_pid(Name) ->
    R = case  ets:lookup(?PPG2TABLE, Name) of
        [] -> {error, {no_process, Name}};
        [{Name, []}] -> {error, {no_process, Name}};
        [{Name, Members}] ->
            {_,_,X} = os:timestamp(),
            lists:nth((X rem length(Members))+1, Members)
    end,
    R
.

process_leave({Name, Pid}, Map) ->
    try
        begin
            {Name, Ref} = dict:fetch(Pid, Map),
            erlang:demonitor(Ref),
            case  ets:lookup(?PPG2TABLE, Name) of
                [] ->
                    ok;
                [{Name, Set}]  ->
                    Set2 = lists:delete(Pid, Set),
                    ets:insert(?PPG2TABLE, {Name, Set2})
            end,
            dict:erase(Pid, Map)
        end
    catch
        _:_ -> Map
    end
.



%%%
%%% Callback functions from gen_server
%%%

-record(state, {}).

-type state() :: #state{}.

-spec init(Arg :: []) -> {'ok', state()}.

init([]) ->
    ets:new(?PPG2TABLE, [set, named_table]),
    {ok, {dict:new()}}
.

-spec handle_call(Call :: {'create', Name}
                        | {'delete', Name}
                        | {'join', Name, Pid :: pid()}
                        | {'leave', Name, Pid :: pid()},
                  From :: {pid(),Tag :: any()},
                  State :: state()) -> {'reply', 'ok', state()}
      when Name :: name().

handle_call({join, Name, Pid}, _From, _S={Map}) ->
    Map2 = try
        case  ets:lookup(?PPG2TABLE, Name) of
            [] ->
                ets:insert(?PPG2TABLE, {Name, [Pid]}),
                Ref = erlang:monitor(process, Pid),
                dict:store(Pid, {Name,Ref}, Map);
            [{Name, Set}]  ->
                case lists:member(Pid, Set) of
                    false  ->
                       ets:insert(?PPG2TABLE, {Name, [Pid|Set]}),
                       Ref = erlang:monitor(process, Pid),
                       dict:store(Pid, {Name,Ref}, Map);
                    true ->
                       Map
                end
        end
    catch _:_ ->
         Map
    end,
    {reply, ok, {Map2}}
;

handle_call({leave, Name, Pid}, _From, _S={Map}) ->
    {reply, ok, {process_leave({Name, Pid}, Map)}};

handle_call({get_members, Name}, _From, S) ->
    Set = case  ets:lookup(?PPG2TABLE, Name) of
        [] -> [];
        [{Name, List}] -> List
    end,
    {reply, {ok, Set}, S};

handle_call({get_closest_pid, Name}, _From, S) ->
    R = case  ets:lookup(?PPG2TABLE, Name) of
        [] -> {error, {no_process, Name}};
        [{Name, Members}] ->
            {_,_,X} = os:timestamp(),
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

handle_info({'DOWN', _MonitorRef, process, Pid, _Info}, _S={Map}) ->
    {Name, _Ref} = dict:fetch(Pid, Map),
    Map2 = process_leave({Name, Pid}, Map),
    {noreply, {Map2}};

handle_info(_, S) ->
    {noreply, S}.

code_change(_OldVersion, Library, _Extra) -> {ok, Library}.

-spec terminate(Reason :: any(), State :: state()) -> 'ok'.

terminate(_Reason, _S) ->
ok.
