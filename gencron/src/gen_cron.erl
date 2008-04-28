%% @doc Provides a gen_server-like interface to periodic execution.
%% The {@link handle_tick/2} callback is executed in a seperate 
%% process periodically or in response to a {@link force_run/1} command.
%% Only one instance of a spawned handle_tick/2 is allowed at any time.
%% @end

-module (gen_cron).
-export ([ force_run/1,
           start/4,
           start/5,
           start_link/4,
           start_link/5 ]).
%-behaviour (behaviour).
-export ([ behaviour_info/1 ]).
-behaviour (gen_server).
-export ([ init/1,
           handle_call/3,
           handle_cast/2,
           handle_info/2,
           terminate/2,
           code_change/3 ]).
-export ([ handle_tick/2 ]).

-include_lib ("eunit/include/eunit.hrl").

-define (is_timeout (X), (((X) =:= infinity) orelse 
                          (is_integer (X) andalso (X) > 0))).

%-=====================================================================-
%-                                Public                               -
%-=====================================================================-

%% @hidden

behaviour_info (callbacks) ->
  [ { init, 1 },
    { handle_call, 3 },
    { handle_cast, 2 },
    { handle_info, 2 },
    { terminate, 2 },
    { code_change, 3 },
    { handle_tick, 2 } ];
behaviour_info (_Other) ->
  undefined.

%% @spec force_run (ServerRef) -> { ok, Pid } | { underway, Pid }
%% @doc Schedule an immediate execution.  If the process is already
%% executing then { underway, Pid } is returned.
%% @end

force_run (ServerRef) ->
  gen_server:call (ServerRef, { ?MODULE, force_run }).

%% @spec start (Module, Interval::integer (), Args, Options) -> Result
%% @doc The analog to gen_server:start/3.  Takes an extra argument
%% Interval which is the periodic execution interval in milliseconds.
%% @end

start (Module, Interval, Args, Options) when ?is_timeout (Interval) ->
  gen_server:start (?MODULE, [ Module, Interval | Args ], Options).

%% @spec start (ServerName, Module, Interval::integer (), Args, Options) -> Result
%% @doc The analog to gen_server:start/4.  Takes an extra argument
%% Interval which is the periodic execution interval in milliseconds.
%% @end

start (ServerName, Module, Interval, Args, Options) when ?is_timeout (Interval) ->
  gen_server:start (ServerName, ?MODULE, [ Module, Interval | Args ], Options).

%% @spec start_link (Module, Interval::integer (), Args, Options) -> Result
%% @doc The analog to gen_server:start_link/3.  Takes an extra argument
%% Interval which is the periodic execution interval in milliseconds.
%% @end

start_link (Module, Interval, Args, Options) when ?is_timeout (Interval) ->
  gen_server:start_link (?MODULE, [ Module, Interval | Args ], Options).

%% @spec start_link (ServerName, Module, Interval::integer (), Args, Options) -> Result
%% @doc The analog to gen_server:start_link/4.  Takes an extra argument
%% Interval which is the periodic execution interval in milliseconds.
%% @end

start_link (ServerName, Module, Interval, Args, Options) when ?is_timeout (Interval) ->
  gen_server:start_link (ServerName, 
                         ?MODULE,
                         [ Module, Interval | Args ],
                         Options).

%-=====================================================================-
%-                         gen_server callbacks                        -
%-=====================================================================-

-record (gencron, { module,
                    mref = undefined,
                    pid = undefined,
                    state }).

%% @spec init (Args) -> result ()
%% @doc Just like the gen_server version.
%% @end

init ([ Module, Interval | Args ]) ->
  process_flag (trap_exit, true),

  case Module:init (Args) of
    { ok, State } ->
      case Interval of
        infinity -> ok;
        _ -> { ok, _ } = timer:send_interval (Interval, { ?MODULE, tick })
      end,
      { ok, #gencron{ module = Module, state = State } };
    { ok, State, Timeout } ->
      case Interval of
        infinity -> ok;
        _ -> { ok, _ } = timer:send_interval (Interval, { ?MODULE, tick })
      end,
      { ok, #gencron{ module = Module, state = State }, Timeout };
    R ->
      R
  end.

%% @spec handle_call (Request, From, State) -> Result
%% @doc Just like the gen_server version, except that 
%% the force_run call is intercepted and handled.
%% @end

handle_call ({ ?MODULE, force_run }, 
             _From,
             State = #gencron{ pid = undefined }) ->
  { Pid, MRef } = erlang:spawn_monitor (State#gencron.module,
                                        handle_tick,
                                        [ force, State#gencron.state ]),
  { reply, { ok, Pid }, State#gencron{ pid = Pid, mref = MRef } };
handle_call ({ ?MODULE, force_run }, _From, State) ->
  { reply, { underway, State#gencron.pid }, State };
handle_call (Request, From, State) ->
  wrap ((State#gencron.module):handle_call (Request,
                                            From,
                                            State#gencron.state),
        State).

%% @spec handle_cast (Request, State) -> Result
%% @doc Just like the gen_server version.
%% @end

handle_cast (Request, State) ->
  wrap ((State#gencron.module):handle_cast (Request, State#gencron.state),
        State).

%% @spec handle_info (Msg, State) -> Result
%% @doc Just like the gen_server version, except that 
%% messages related to spawned process monitor are intercepted and
%% handled (and forwarded to the callback module in a { tick_monitor, Msg }
%% tuple), and messages related to the periodic timer are handled 
%% (and not forwarded to the callback module).
%% @end

handle_info (Msg = { 'DOWN', MRef, _, _, _ }, State = #gencron{ mref = MRef }) ->
  handle_info ({ tick_monitor, Msg },
               State#gencron{ pid = undefined, mref = undefined });
handle_info ({ ?MODULE, tick }, State = #gencron{ mref = undefined }) ->
  { Pid, MRef } = erlang:spawn_monitor (State#gencron.module,
                                        handle_tick,
                                        [ tick, State#gencron.state ]),

  { noreply, State#gencron{ pid = Pid, mref = MRef } };
handle_info ({ ?MODULE, tick }, State) ->
  { noreply, State };
handle_info (Msg, State) ->
  wrap ((State#gencron.module):handle_info (Msg, State#gencron.state), State).

%% @spec terminate (Result, State) -> Result
%% @doc Just like the gen_server version, except that 
%% if a process is running, we wait for it to terminate
%% (after calling the module's terminate).
%% @end

terminate (Reason, State) ->
  (State#gencron.module):terminate (Reason, State#gencron.state),
  case State#gencron.mref of
    undefined -> ok;
    MRef -> receive { 'DOWN', MRef, _, _, _ } -> ok end
  end.

%% @spec code_change (OldVsn, State, Extra) -> Result
%% @doc Just like the gen_server version.
%% @end

code_change (OldVsn, State, Extra) -> 
  { ok, NewState } = (State#gencron.module):code_change (OldVsn,
                                                         State#gencron.state,
                                                         Extra),
  { ok, State#gencron{ state = NewState } }.

%% @spec handle_tick (Reason::reason (), State) -> none ()
%%   reason () = tick | force
%% @doc This is called as a seperate process, either periodically or in
%% response to a force.  The State argument is the server state at 
%% the time of the spawn.

handle_tick (_Reason, _State) ->
  ok.

%-=====================================================================-
%-                               Private                               -
%-=====================================================================-

wrap ({ reply, Reply, NewState }, State) ->
  { reply, Reply, State#gencron{ state = NewState } };
wrap ({ reply, Reply, NewState, Timeout }, State) ->
  { reply, Reply, State#gencron{ state = NewState }, Timeout };
wrap ({ noreply, NewState }, State) ->
  { noreply, State#gencron{ state = NewState } };
wrap ({ noreply, NewState, Timeout }, State) ->
  { noreply, State#gencron{ state = NewState }, Timeout };
wrap ({ stop, Reason, Reply, NewState }, State) ->
  { stop, Reason, Reply, State#gencron{ state = NewState } };
wrap ({ stop, Reason, NewState }, State) ->
  { stop, Reason, State#gencron{ state = NewState } }.

-ifdef (EUNIT).

%-=====================================================================-
%-                                Tests                                -
%-=====================================================================-

infinity_test_ () ->
  F = 
    fun (Tab) ->
      fun () ->
        { ok, Pid } = gen_cron:start_link (gen_cron_test,
                                           infinity,
                                           [ Tab ],
                                           [ ]),
  
        MRef = erlang:monitor (process, Pid),
  
        { ok, RunPid } = gen_cron:force_run (Pid),
        { underway, RunPid } = gen_cron:force_run (Pid),
  
        receive after 6500 -> ok end,
  
        exit (Pid, shutdown),
  
        receive
          { 'DOWN', MRef, _, _, _ } -> ok
        end,
  
        ?assert (ets:lookup (Tab, count) =:= [{ count, 0 }]),
        ?assert (ets:lookup (Tab, force) =:= [{ force, 1 }]),
        ?assert (ets:lookup (Tab, mega) =:= []),
        ?assert (ets:lookup (Tab, turg) =:= []),
        true
      end
    end,

    { setup,
      fun () -> ets:new (?MODULE, [ public, set ]) end,
      fun (Tab) -> ets:delete (Tab) end,
      fun (Tab) -> { timeout, 60, F (Tab) } end
    }.

tick_test_ () ->
  F = 
    fun (Tab) ->
      fun () ->
        { ok, Pid } = gen_cron:start_link (gen_cron_test, 1000, [ Tab ], [ ]),
  
        MRef = erlang:monitor (process, Pid),
  
        { ok, RunPid } = gen_cron:force_run (Pid),
        { underway, RunPid } = gen_cron:force_run (Pid),
  
        receive after 6500 -> ok end,
  
        exit (Pid, shutdown),
  
        receive
          { 'DOWN', MRef, _, _, Info } -> ?assert (Info =:= shutdown)
        end,

        ?assert (ets:lookup (Tab, count) =:= [{ count, 2 }]),
        ?assert (ets:lookup (Tab, force) =:= [{ force, 1 }]),
        ?assert (ets:lookup (Tab, tick_monitor) =:= [{ tick_monitor, 2 }]),
        ?assert (ets:lookup (Tab, mega) =:= []),
        ?assert (ets:lookup (Tab, turg) =:= []),
        true
      end
    end,

    { setup,
      fun () -> ets:new (?MODULE, [ public, set ]) end,
      fun (Tab) -> ets:delete (Tab) end,
      fun (Tab) -> { timeout, 60, F (Tab) } end
    }.

call_test_ () ->
  F = 
    fun (Tab) ->
      fun () ->
        { ok, Pid } = gen_cron:start_link (gen_cron_test,
                                           infinity,
                                           [ Tab ],
                                           [ ]),
  
        MRef = erlang:monitor (process, Pid),
  
        flass = gen_server:call (Pid, turg),
        flass = gen_server:call (Pid, turg),
  
        exit (Pid, shutdown),
  
        receive
          { 'DOWN', MRef, _, _, Info } -> ?assert (Info =:= shutdown)
        end,
  
        ?assert (ets:lookup (Tab, count) =:= [{ count, 0 }]),
        ?assert (ets:lookup (Tab, force) =:= [{ force, 0 }]),
        ?assert (ets:lookup (Tab, tick_monitor) =:= [{ tick_monitor, 0 }]),
        ?assert (ets:lookup (Tab, mega) =:= []),
        ?assert (ets:lookup (Tab, turg) =:= [{ turg, baitin }]),
        true
      end
    end,

    { setup,
      fun () -> ets:new (?MODULE, [ public, set ]) end,
      fun (Tab) -> ets:delete (Tab) end,
      fun (Tab) -> { timeout, 60, F (Tab) } end
    }.

info_test_ () ->
  F = 
    fun (Tab) ->
      fun () ->
        { ok, Pid } = gen_cron:start_link (gen_cron_test,
                                           infinity,
                                           [ Tab ],
                                           [ ]),
  
        MRef = erlang:monitor (process, Pid),
  
        Pid ! { mega, sweet },
        Pid ! { mega, sweet },
  
        exit (Pid, shutdown),
  
        receive
          { 'DOWN', MRef, _, _, Info } -> ?assert (Info =:= shutdown)
        end,
  
        ?assert (ets:lookup (Tab, count) =:= [{ count, 0 }]),
        ?assert (ets:lookup (Tab, force) =:= [{ force, 0 }]),
        ?assert (ets:lookup (Tab, tick_monitor) =:= [{ tick_monitor, 0 }]),
        ?assert (ets:lookup (Tab, mega) =:= [{ mega, sweet }]),
        ?assert (ets:lookup (Tab, turg) =:= []),
        true
      end
    end,

    { setup,
      fun () -> ets:new (?MODULE, [ public, set ]) end,
      fun (Tab) -> ets:delete (Tab) end,
      fun (Tab) -> { timeout, 60, F (Tab) } end
    }.

-endif.
