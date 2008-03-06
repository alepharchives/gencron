-module (gen_expire).
-export ([ force_run/1,
           start/4,
           start/5,
           start_link/4,
           start_link/5 ]).
%-behaviour (behaviour).
-export ([ behaviour_info/1 ]).
-behaviour (gen_cron).
-export ([ init/1,
           handle_call/3,
           handle_cast/2,
           handle_info/2,
           terminate/2,
           code_change/3,
           handle_tick/2 ]).
%-behaviour (gen_expire).
-export ([ activity_context/2,
           first/2,
           next/3,
           delete/3,
           finish/1 ]).

-include_lib ("flasscheck/include/quickcheck.hrl").
-include_lib ("eunit/include/eunit.hrl").
-include ("gen_expire.hrl").

-define (is_timeout (X), (((X) =:= infinity) orelse 
                          (is_integer (X) andalso (X) > 0))).

-record (genexpire, { module, speclist, state }).

%% @type expirespec() = { expirespec, Table::atom (), MaxBytesPerBox::integer () }.  This is the record type #expirespec{ table, max_bytes_per_box }.

%-=====================================================================-
%-                                Public                               -
%-=====================================================================-

%% @hidden

behaviour_info (callbacks) ->
  [ { activity_context, 2 },
    { first, 2 },
    { next, 3 },
    { delete, 3 },
    { finish, 1 },
    { init, 1 },
    { handle_call, 3 },
    { handle_cast, 2 },
    { handle_info, 2 },
    { terminate, 2 },
    { code_change, 3 } ];
behaviour_info (_Other) ->
  undefined.

%% @spec force_run (ServerRef) -> { ok, Pid } | { underway, Pid }
%% @doc Schedule an immediate expiration.  If the process is already
%% executing then { underway, Pid } is returned.
%% @end

force_run (ServerRef) ->
  gen_cron:force_run (ServerRef).

%% @spec start (Module, Interval::integer (), Args, Options) -> Result
%% @doc The analog to gen_server:start/3.  Takes an extra argument
%% Interval which is the periodic expiration interval in milliseconds.
%% @end

start (Module, Interval, Args, Options) when ?is_timeout (Interval) ->
  gen_cron:start (?MODULE, Interval, [ Module | Args ], Options).

%% @spec start (ServerName, Module, Interval::integer (), Args, Options) -> Result
%% @doc The analog to gen_server:start/4.  Takes an extra argument
%% Interval which is the periodic expiration interval in milliseconds.
%% @end

start (ServerName, Module, Interval, Args, Options) when ?is_timeout (Interval) ->
  gen_cron:start (ServerName, ?MODULE, Interval, [ Module | Args ], Options).

%% @spec start_link (Module, Interval::integer (), Args, Options) -> Result
%% @doc The analog to gen_server:start_link/3.  Takes an extra argument
%% Interval which is the periodic expiration interval in milliseconds.
%% @end

start_link (Module, Interval, Args, Options) when ?is_timeout (Interval) ->
  gen_cron:start_link (?MODULE, Interval, [ Module | Args ], Options).

%% @spec start_link (ServerName, Module, Interval::integer (), Args, Options) -> Result
%% @doc The analog to gen_server:start_link/4.  Takes an extra argument
%% Interval which is the periodic expiration interval in milliseconds.
%% @end

start_link (ServerName, Module, Interval, Args, Options) when ?is_timeout (Interval) ->
  gen_cron:start_link (ServerName,
                       ?MODULE,
                       Interval,
                       [ Module | Args ],
                       Options).

%-=====================================================================-
%-                         gen_expire callbacks                        -
%-=====================================================================-

%% @spec activity_context (atom (), state ()) -> { async_dirty | sync_dirty | transaction | sync_transaction | ets, NewState::state () }
%% @doc Indicate what activity context to use for expiring table Table.  Note
%% that gen_expire holds a global lock on Table which precludes 
%% other gen_expire instances from expiring the table simultaneously.
%% @end

activity_context (_Table, _State) ->
  erlang:throw (not_implemented).

%% @spec first (atom (), state ()) -> { ok, Key::any (), NewState::state () } | { end_of_table, NewState::state () } 
%% @doc Retrieve the first key from the table (fragment).  "First" here means
%% "in order of desired expiration".
%% @end

first (_Table, _State) ->
  erlang:throw (not_implemented).

%% @spec next (atom (), any (), state ()) -> { ok, NextKey::any (), NewState::state () } | { end_of_table, NewState::state () }
%% @doc Retrieve the next key from the table (fragment).  "Next" here means
%% "in order of desired expiration".
%% @end

next (_Table, _Key, _State) ->
  erlang:throw (not_implemented).

%% @spec delete (atom (), any (), state ()) -> { ok, NewState::state () }
%% @doc Delete the specified key from the table (fragment).
%% @end

delete (_Table, _Key, _State) ->
  erlang:throw (not_implemented).

%% @spec finish (state ()) -> void
%% @doc Called when gen_expire has finished an expiration run.
%% Note that while the state is threaded through
%% a complete expiration run, state changes at the end of the expiration
%% run are discarded because the expiration occurs in seperate process.
%% Therefore this is the opportunity to record any interesting state 
%% changes, e.g., log statistics.
%% @end

finish (_State) ->
  erlang:throw (not_implemented).

%-=====================================================================-
%-                          gen_cron callbacks                         -
%-=====================================================================-

%% @spec init (Args) -> result ()
%%   result () = { ok, Tabs::list (#expirespec{}), State::any () } |
%%               { ok, Tabs::list (#expirespec{}), State::any (), Timeout::integer () } |
%%               { stop, Reason::any () } |
%%               ignore
%% @doc Initialization routine.  Like Module:init/1 for gen_server, except
%% that a list of #expirespec{} is returned to control expiration behavior.
%% @end

init ([ Module | Args ]) ->
  case Module:init (Args) of
    { ok, Speclist, State } ->
      { ok, #genexpire{ module = Module, speclist = Speclist, state = State } };
    { ok, Speclist, State, Timeout } ->
      { ok, 
        #genexpire{ module = Module, speclist = Speclist, state = State },
        Timeout };
    R ->
      R
  end.

%% @spec handle_call (Request, From, State) -> Result
%% @doc Just like the gen_server version.
%% @end

handle_call (Request, From, State) ->
  wrap ((State#genexpire.module):handle_call (Request,
                                              From,
                                              State#genexpire.state),
        State).

%% @spec handle_cast (Request, State) -> Result
%% @doc Just like the gen_server version.
%% @end

handle_cast (Request, State) ->
  wrap ((State#genexpire.module):handle_cast (Request, State#genexpire.state),
        State).

%% @spec handle_info (Msg, State) -> Result
%% @doc Just like the gen_server version.
%% @end

handle_info (Msg, State) ->
  wrap ((State#genexpire.module):handle_info (Msg, State#genexpire.state),
        State).

%% @spec code_change (OldVsn, State, Extra) -> Result
%% @doc Just like the gen_server version.
%% @end

code_change (OldVsn, State, Extra) -> 
  { ok, NewState } = 
    (State#genexpire.module):code_change (OldVsn,
                                          State#genexpire.state,
                                          Extra),
  { ok, State#genexpire{ state = NewState } }.

%% @spec terminate (Result, State) -> Result
%% @doc Just like the gen_server version, except that
%% if a process is running, we wait for it to terminate
%% (prior to calling the module's terminate).
%% @end

terminate (Reason, State) ->
  (State#genexpire.module):terminate (Reason, State#genexpire.state).

%% @hidden

handle_tick (_Reason, State) ->
  FinalState = 
    lists:foldl (fun (#expirespec{ table = Table, 
                                   max_bytes_per_box = MaxBytes },
                      State2) ->
                   LocalFragments = local_fragments (Table),

                   case LocalFragments of
                     [] ->
                       State2;
                     _ ->
                       MaxFragBytes = MaxBytes div length (LocalFragments),
                       lists:foldl (fun (F, State3) ->
                                      expire_frag (F, MaxFragBytes, State3)
                                    end,
                                    State2,
                                    LocalFragments)
                   end
                 end,
                 State,
                 State#genexpire.speclist),
  (FinalState#genexpire.module):finish (FinalState#genexpire.state).

%-=====================================================================-
%-                               Private                               -
%-=====================================================================-

expire_frag (Table, MaxFragBytes, State) ->
  LockId = { { ?MODULE, Table }, self () },

  global:set_lock (LockId),

  try
    { Context, NewState } = 
      (State#genexpire.module):activity_context (Table, State#genexpire.state), 
    mnesia:activity 
      (Context,
       fun () ->
         First = (State#genexpire.module):first (Table, NewState),
         
         case First of
           { end_of_table, NewState2 } -> 
             State#genexpire{ state = NewState2 };
           { ok, _, NewState2 } ->
              expire_frag (Table,
                           MaxFragBytes,
                           State#genexpire{ state = NewState2 },
                           First)
         end
       end)
  after
    global:del_lock (LockId)
  end.

expire_frag (_Table, _MaxFragBytes, State, { end_of_table, _ }) ->
  State;
expire_frag (Table, MaxFragBytes, State, { ok, Key, _ }) ->
  case mnesia:table_info (Table, memory) of
    N when N > MaxFragBytes ->
      Next = (State#genexpire.module):next (Table, Key, State#genexpire.state),
      NewState = case Next of { end_of_table, X } -> X; { ok, _, X } -> X end,

      { ok, NewState2 } = (State#genexpire.module):delete (Table,
                                                           Key,
                                                           NewState),

      expire_frag (Table, 
                   MaxFragBytes, 
                   State#genexpire{ state = NewState2 },
                   Next);
    _ ->
      State
  end.

frag_table_name (Tab, 1) ->
  Tab;
frag_table_name (Tab, N) ->
  list_to_atom (atom_to_list (Tab) ++ "_frag" ++ integer_to_list (N)).

is_local (TableName) ->
  lists:member (node (), mnesia:table_info (TableName, disc_copies)) orelse
  lists:member (node (), mnesia:table_info (TableName, disc_only_copies)) orelse
  lists:member (node (), mnesia:table_info (TableName, ram_copies)).
                       
local_fragments (TableName) ->
  [ F || N <- lists:seq (1, num_fragments (TableName)),
         F <- [ frag_table_name (TableName, N) ],
         is_local (F) ].

num_fragments (Tablename) ->
  { value, { n_fragments, N } } =
    lists:keysearch (n_fragments,
                     1,
                     mnesia:table_info (Tablename, frag_properties)),
  N.

wrap ({ reply, Reply, NewState }, State) ->
  { reply, Reply, State#genexpire{ state = NewState } };
wrap ({ reply, Reply, NewState, Timeout }, State) ->
  { reply, Reply, State#genexpire{ state = NewState }, Timeout };
wrap ({ noreply, NewState }, State) ->
  { noreply, State#genexpire{ state = NewState } };
wrap ({ noreply, NewState, Timeout }, State) ->
  { noreply, State#genexpire{ state = NewState }, Timeout };
wrap ({ stop, Reason, Reply, NewState }, State) ->
  { stop, Reason, Reply, State#genexpire{ state = NewState } };
wrap ({ stop, Reason, NewState }, State) ->
  { stop, Reason, State#genexpire{ state = NewState } }.

-ifdef (EUNIT).

random_atom (Size) ->
  list_to_atom (random_string (Size)).

random_string (Size) ->
  [ $a + random:uniform ($z - $a) - 1 || _ <- lists:seq (1, Size) ].

%-=====================================================================-
%-                                Tests                                -
%-=====================================================================-

expire_test_ () ->
  F = fun () ->
    T = ?FORALL (X,
                 fun (Size) -> 
                  { random_atom (Size), 
                    random:uniform (Size),
                    random:uniform (8) =:= 1,
                    case random:uniform (8) of 
                      1 -> all;
                      2 -> none;
                      _ -> random:uniform (Size)
                    end,
                    [ { N, random_string (Size) } 
                      || N <- lists:seq (1, Size) ] }
                 end,
                 (fun ({ Tab, Frags, Empty, Keep, Terms }) ->
                    TabDup = list_to_atom (atom_to_list (Tab) ++ "_dup"),

                    { atomic, ok } = 
                       mnesia:create_table (Tab, 
                                            [ { frag_properties, [ 
                                                { n_fragments, Frags } ] } ]),

                    { atomic, ok } = 
                       mnesia:create_table (TabDup, 
                                            [ { record_name, Tab },
                                              { frag_properties, [ 
                                                { n_fragments, Frags } ] } ]),

                    InitSize = mnesia:table_info (Tab, memory),

                    if Empty -> 
                         Sizes = [];
                       true ->
                         Sizes = 
                           [ begin
                               mnesia:dirty_write (Tab, { Tab, Key, Value }),
                               mnesia:dirty_write (TabDup, { Tab, Key, Value }),
                               mnesia:table_info (Tab, memory)
                             end ||
                             { Key, Value } <- Terms ]
                    end,

                    { ok, Pid } = 
                       gen_expire_test:start_link 
                         (1000,
                          case { Keep, Empty } of
                            { _, true } -> Frags * InitSize;
                            { all, false } -> Frags * lists:last (Sizes);
                            { none, false } -> Frags * InitSize;
                            { _, false } -> Frags * lists:nth (Keep, Sizes)
                          end,
                          Tab),

                    { ok, ExpPid } = gen_expire:force_run (Pid),
                    { underway, ExpPid } = gen_expire:force_run (Pid),

                    MRef = erlang:monitor (process, ExpPid),

                    receive
                      { 'DOWN', MRef, _, _, _ } -> ok
                    end,

                    ?assert (mnesia:table_info (Tab, memory) =:=
                             case { Keep, Empty } of
                               { _, true } -> InitSize;
                               { all, false } -> lists:last (Sizes);
                               { none, false } -> InitSize;
                               { _, false } -> lists:nth (Keep, Sizes)
                             end),

                    ?assert (mnesia:table_info (TabDup, memory) =:=
                             mnesia:table_info (Tab, memory)),

                    gen_expire_test:stop (Pid),

                    MRef2 = erlang:monitor (process, Pid),

                    receive
                      { 'DOWN', MRef2, _, _, _ } -> ok
                    end,

                    mnesia:delete_table (Tab),
                    mnesia:delete_table (TabDup),

                    true
                  end) (X)),

    ok = fc:flasscheck (200, 10, T)
  end,

  { setup,
    fun () -> os:cmd ("rm -rf Mnesia*"), mnesia:start () end,
    fun (_) -> mnesia:stop (), os:cmd ("rm -rf Mnesia*") end,
    { timeout, 60, F } 
  }.

-endif.
