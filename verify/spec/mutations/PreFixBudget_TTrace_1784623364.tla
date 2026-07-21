---- MODULE PreFixBudget_TTrace_1784623364 ----
EXTENDS Sequences, TLCExt, PreFixBudget, Toolbox, Naturals, TLC

_expression ==
    LET PreFixBudget_TEExpression == INSTANCE PreFixBudget_TEExpression
    IN PreFixBudget_TEExpression!expression
----

_trace ==
    LET PreFixBudget_TETrace == INSTANCE PreFixBudget_TETrace
    IN PreFixBudget_TETrace!trace
----

_inv ==
    ~(
        TLCGet("level") = Len(_TETrace)
        /\
        journaled = (<<{"b1", "b2", "b3", "b4"}, {}, {}, {}>>)
        /\
        cancelReq = (<<FALSE, FALSE, FALSE, FALSE>>)
        /\
        freshAttempt = (<<FALSE, FALSE, FALSE, FALSE>>)
        /\
        spent = (<<8, 0, 0, 0>>)
        /\
        execCount = (<<[b1 |-> 1, b2 |-> 1, b3 |-> 1, b4 |-> 1, t1 |-> 0, s1 |-> 0, s2 |-> 0], [b1 |-> 0, b2 |-> 0, b3 |-> 0, b4 |-> 0, t1 |-> 0, s1 |-> 0, s2 |-> 0], [b1 |-> 0, b2 |-> 0, b3 |-> 0, b4 |-> 0, t1 |-> 0, s1 |-> 0, s2 |-> 0], [b1 |-> 0, b2 |-> 0, b3 |-> 0, b4 |-> 0, t1 |-> 0, s1 |-> 0, s2 |-> 0]>>)
        /\
        clock = (0)
        /\
        state = (<<"running", "absent", "absent", "absent">>)
        /\
        attempt = (<<2, 0, 0, 0>>)
        /\
        live = (<<TRUE, FALSE, FALSE, FALSE>>)
    )
----

_init ==
    /\ freshAttempt = _TETrace[1].freshAttempt
    /\ clock = _TETrace[1].clock
    /\ state = _TETrace[1].state
    /\ execCount = _TETrace[1].execCount
    /\ journaled = _TETrace[1].journaled
    /\ spent = _TETrace[1].spent
    /\ cancelReq = _TETrace[1].cancelReq
    /\ live = _TETrace[1].live
    /\ attempt = _TETrace[1].attempt
----

_next ==
    /\ \E i,j \in DOMAIN _TETrace:
        /\ \/ /\ j = i + 1
              /\ i = TLCGet("level")
        /\ freshAttempt  = _TETrace[i].freshAttempt
        /\ freshAttempt' = _TETrace[j].freshAttempt
        /\ clock  = _TETrace[i].clock
        /\ clock' = _TETrace[j].clock
        /\ state  = _TETrace[i].state
        /\ state' = _TETrace[j].state
        /\ execCount  = _TETrace[i].execCount
        /\ execCount' = _TETrace[j].execCount
        /\ journaled  = _TETrace[i].journaled
        /\ journaled' = _TETrace[j].journaled
        /\ spent  = _TETrace[i].spent
        /\ spent' = _TETrace[j].spent
        /\ cancelReq  = _TETrace[i].cancelReq
        /\ cancelReq' = _TETrace[j].cancelReq
        /\ live  = _TETrace[i].live
        /\ live' = _TETrace[j].live
        /\ attempt  = _TETrace[i].attempt
        /\ attempt' = _TETrace[j].attempt

\* Uncomment the ASSUME below to write the states of the error trace
\* to the given file in Json format. Note that you can pass any tuple
\* to `JsonSerialize`. For example, a sub-sequence of _TETrace.
    \* ASSUME
    \*     LET J == INSTANCE Json
    \*         IN J!JsonSerialize("PreFixBudget_TTrace_1784623364.json", _TETrace)

=============================================================================

 Note that you can extract this module `PreFixBudget_TEExpression`
  to a dedicated file to reuse `expression` (the module in the 
  dedicated `PreFixBudget_TEExpression.tla` file takes precedence 
  over the module `PreFixBudget_TEExpression` below).

---- MODULE PreFixBudget_TEExpression ----
EXTENDS Sequences, TLCExt, PreFixBudget, Toolbox, Naturals, TLC

expression == 
    [
        \* To hide variables of the `PreFixBudget` spec from the error trace,
        \* remove the variables below.  The trace will be written in the order
        \* of the fields of this record.
        freshAttempt |-> freshAttempt
        ,clock |-> clock
        ,state |-> state
        ,execCount |-> execCount
        ,journaled |-> journaled
        ,spent |-> spent
        ,cancelReq |-> cancelReq
        ,live |-> live
        ,attempt |-> attempt
        
        \* Put additional constant-, state-, and action-level expressions here:
        \* ,_stateNumber |-> _TEPosition
        \* ,_freshAttemptUnchanged |-> freshAttempt = freshAttempt'
        
        \* Format the `freshAttempt` variable as Json value.
        \* ,_freshAttemptJson |->
        \*     LET J == INSTANCE Json
        \*     IN J!ToJson(freshAttempt)
        
        \* Lastly, you may build expressions over arbitrary sets of states by
        \* leveraging the _TETrace operator.  For example, this is how to
        \* count the number of times a spec variable changed up to the current
        \* state in the trace.
        \* ,_freshAttemptModCount |->
        \*     LET F[s \in DOMAIN _TETrace] ==
        \*         IF s = 1 THEN 0
        \*         ELSE IF _TETrace[s].freshAttempt # _TETrace[s-1].freshAttempt
        \*             THEN 1 + F[s-1] ELSE F[s-1]
        \*     IN F[_TEPosition - 1]
    ]

=============================================================================



Parsing and semantic processing can take forever if the trace below is long.
 In this case, it is advised to uncomment the module below to deserialize the
 trace from a generated binary file.

\*
\*---- MODULE PreFixBudget_TETrace ----
\*EXTENDS IOUtils, PreFixBudget, TLC
\*
\*trace == IODeserialize("PreFixBudget_TTrace_1784623364.bin", TRUE)
\*
\*=============================================================================
\*

---- MODULE PreFixBudget_TETrace ----
EXTENDS PreFixBudget, TLC

trace == 
    <<
    ([journaled |-> <<{}, {}, {}, {}>>,cancelReq |-> <<FALSE, FALSE, FALSE, FALSE>>,freshAttempt |-> <<FALSE, FALSE, FALSE, FALSE>>,spent |-> <<0, 0, 0, 0>>,execCount |-> <<[b1 |-> 0, b2 |-> 0, b3 |-> 0, b4 |-> 0, t1 |-> 0, s1 |-> 0, s2 |-> 0], [b1 |-> 0, b2 |-> 0, b3 |-> 0, b4 |-> 0, t1 |-> 0, s1 |-> 0, s2 |-> 0], [b1 |-> 0, b2 |-> 0, b3 |-> 0, b4 |-> 0, t1 |-> 0, s1 |-> 0, s2 |-> 0], [b1 |-> 0, b2 |-> 0, b3 |-> 0, b4 |-> 0, t1 |-> 0, s1 |-> 0, s2 |-> 0]>>,clock |-> 0,state |-> <<"absent", "absent", "absent", "absent">>,attempt |-> <<0, 0, 0, 0>>,live |-> <<FALSE, FALSE, FALSE, FALSE>>]),
    ([journaled |-> <<{}, {}, {}, {}>>,cancelReq |-> <<FALSE, FALSE, FALSE, FALSE>>,freshAttempt |-> <<FALSE, FALSE, FALSE, FALSE>>,spent |-> <<0, 0, 0, 0>>,execCount |-> <<[b1 |-> 0, b2 |-> 0, b3 |-> 0, b4 |-> 0, t1 |-> 0, s1 |-> 0, s2 |-> 0], [b1 |-> 0, b2 |-> 0, b3 |-> 0, b4 |-> 0, t1 |-> 0, s1 |-> 0, s2 |-> 0], [b1 |-> 0, b2 |-> 0, b3 |-> 0, b4 |-> 0, t1 |-> 0, s1 |-> 0, s2 |-> 0], [b1 |-> 0, b2 |-> 0, b3 |-> 0, b4 |-> 0, t1 |-> 0, s1 |-> 0, s2 |-> 0]>>,clock |-> 0,state |-> <<"ready", "absent", "absent", "absent">>,attempt |-> <<0, 0, 0, 0>>,live |-> <<FALSE, FALSE, FALSE, FALSE>>]),
    ([journaled |-> <<{}, {}, {}, {}>>,cancelReq |-> <<FALSE, FALSE, FALSE, FALSE>>,freshAttempt |-> <<TRUE, FALSE, FALSE, FALSE>>,spent |-> <<0, 0, 0, 0>>,execCount |-> <<[b1 |-> 0, b2 |-> 0, b3 |-> 0, b4 |-> 0, t1 |-> 0, s1 |-> 0, s2 |-> 0], [b1 |-> 0, b2 |-> 0, b3 |-> 0, b4 |-> 0, t1 |-> 0, s1 |-> 0, s2 |-> 0], [b1 |-> 0, b2 |-> 0, b3 |-> 0, b4 |-> 0, t1 |-> 0, s1 |-> 0, s2 |-> 0], [b1 |-> 0, b2 |-> 0, b3 |-> 0, b4 |-> 0, t1 |-> 0, s1 |-> 0, s2 |-> 0]>>,clock |-> 0,state |-> <<"running", "absent", "absent", "absent">>,attempt |-> <<1, 0, 0, 0>>,live |-> <<TRUE, FALSE, FALSE, FALSE>>]),
    ([journaled |-> <<{"b1"}, {}, {}, {}>>,cancelReq |-> <<FALSE, FALSE, FALSE, FALSE>>,freshAttempt |-> <<FALSE, FALSE, FALSE, FALSE>>,spent |-> <<2, 0, 0, 0>>,execCount |-> <<[b1 |-> 1, b2 |-> 0, b3 |-> 0, b4 |-> 0, t1 |-> 0, s1 |-> 0, s2 |-> 0], [b1 |-> 0, b2 |-> 0, b3 |-> 0, b4 |-> 0, t1 |-> 0, s1 |-> 0, s2 |-> 0], [b1 |-> 0, b2 |-> 0, b3 |-> 0, b4 |-> 0, t1 |-> 0, s1 |-> 0, s2 |-> 0], [b1 |-> 0, b2 |-> 0, b3 |-> 0, b4 |-> 0, t1 |-> 0, s1 |-> 0, s2 |-> 0]>>,clock |-> 0,state |-> <<"running", "absent", "absent", "absent">>,attempt |-> <<1, 0, 0, 0>>,live |-> <<TRUE, FALSE, FALSE, FALSE>>]),
    ([journaled |-> <<{"b1", "b2"}, {}, {}, {}>>,cancelReq |-> <<FALSE, FALSE, FALSE, FALSE>>,freshAttempt |-> <<FALSE, FALSE, FALSE, FALSE>>,spent |-> <<4, 0, 0, 0>>,execCount |-> <<[b1 |-> 1, b2 |-> 1, b3 |-> 0, b4 |-> 0, t1 |-> 0, s1 |-> 0, s2 |-> 0], [b1 |-> 0, b2 |-> 0, b3 |-> 0, b4 |-> 0, t1 |-> 0, s1 |-> 0, s2 |-> 0], [b1 |-> 0, b2 |-> 0, b3 |-> 0, b4 |-> 0, t1 |-> 0, s1 |-> 0, s2 |-> 0], [b1 |-> 0, b2 |-> 0, b3 |-> 0, b4 |-> 0, t1 |-> 0, s1 |-> 0, s2 |-> 0]>>,clock |-> 0,state |-> <<"running", "absent", "absent", "absent">>,attempt |-> <<1, 0, 0, 0>>,live |-> <<TRUE, FALSE, FALSE, FALSE>>]),
    ([journaled |-> <<{"b1", "b2", "b3"}, {}, {}, {}>>,cancelReq |-> <<FALSE, FALSE, FALSE, FALSE>>,freshAttempt |-> <<FALSE, FALSE, FALSE, FALSE>>,spent |-> <<6, 0, 0, 0>>,execCount |-> <<[b1 |-> 1, b2 |-> 1, b3 |-> 1, b4 |-> 0, t1 |-> 0, s1 |-> 0, s2 |-> 0], [b1 |-> 0, b2 |-> 0, b3 |-> 0, b4 |-> 0, t1 |-> 0, s1 |-> 0, s2 |-> 0], [b1 |-> 0, b2 |-> 0, b3 |-> 0, b4 |-> 0, t1 |-> 0, s1 |-> 0, s2 |-> 0], [b1 |-> 0, b2 |-> 0, b3 |-> 0, b4 |-> 0, t1 |-> 0, s1 |-> 0, s2 |-> 0]>>,clock |-> 0,state |-> <<"running", "absent", "absent", "absent">>,attempt |-> <<1, 0, 0, 0>>,live |-> <<TRUE, FALSE, FALSE, FALSE>>]),
    ([journaled |-> <<{"b1", "b2", "b3"}, {}, {}, {}>>,cancelReq |-> <<FALSE, FALSE, FALSE, FALSE>>,freshAttempt |-> <<FALSE, FALSE, FALSE, FALSE>>,spent |-> <<6, 0, 0, 0>>,execCount |-> <<[b1 |-> 1, b2 |-> 1, b3 |-> 1, b4 |-> 0, t1 |-> 0, s1 |-> 0, s2 |-> 0], [b1 |-> 0, b2 |-> 0, b3 |-> 0, b4 |-> 0, t1 |-> 0, s1 |-> 0, s2 |-> 0], [b1 |-> 0, b2 |-> 0, b3 |-> 0, b4 |-> 0, t1 |-> 0, s1 |-> 0, s2 |-> 0], [b1 |-> 0, b2 |-> 0, b3 |-> 0, b4 |-> 0, t1 |-> 0, s1 |-> 0, s2 |-> 0]>>,clock |-> 0,state |-> <<"ready", "absent", "absent", "absent">>,attempt |-> <<1, 0, 0, 0>>,live |-> <<TRUE, FALSE, FALSE, FALSE>>]),
    ([journaled |-> <<{"b1", "b2", "b3"}, {}, {}, {}>>,cancelReq |-> <<FALSE, FALSE, FALSE, FALSE>>,freshAttempt |-> <<TRUE, FALSE, FALSE, FALSE>>,spent |-> <<6, 0, 0, 0>>,execCount |-> <<[b1 |-> 1, b2 |-> 1, b3 |-> 1, b4 |-> 0, t1 |-> 0, s1 |-> 0, s2 |-> 0], [b1 |-> 0, b2 |-> 0, b3 |-> 0, b4 |-> 0, t1 |-> 0, s1 |-> 0, s2 |-> 0], [b1 |-> 0, b2 |-> 0, b3 |-> 0, b4 |-> 0, t1 |-> 0, s1 |-> 0, s2 |-> 0], [b1 |-> 0, b2 |-> 0, b3 |-> 0, b4 |-> 0, t1 |-> 0, s1 |-> 0, s2 |-> 0]>>,clock |-> 0,state |-> <<"running", "absent", "absent", "absent">>,attempt |-> <<2, 0, 0, 0>>,live |-> <<TRUE, FALSE, FALSE, FALSE>>]),
    ([journaled |-> <<{"b1", "b2", "b3", "b4"}, {}, {}, {}>>,cancelReq |-> <<FALSE, FALSE, FALSE, FALSE>>,freshAttempt |-> <<FALSE, FALSE, FALSE, FALSE>>,spent |-> <<8, 0, 0, 0>>,execCount |-> <<[b1 |-> 1, b2 |-> 1, b3 |-> 1, b4 |-> 1, t1 |-> 0, s1 |-> 0, s2 |-> 0], [b1 |-> 0, b2 |-> 0, b3 |-> 0, b4 |-> 0, t1 |-> 0, s1 |-> 0, s2 |-> 0], [b1 |-> 0, b2 |-> 0, b3 |-> 0, b4 |-> 0, t1 |-> 0, s1 |-> 0, s2 |-> 0], [b1 |-> 0, b2 |-> 0, b3 |-> 0, b4 |-> 0, t1 |-> 0, s1 |-> 0, s2 |-> 0]>>,clock |-> 0,state |-> <<"running", "absent", "absent", "absent">>,attempt |-> <<2, 0, 0, 0>>,live |-> <<TRUE, FALSE, FALSE, FALSE>>])
    >>
----


=============================================================================

---- CONFIG PreFixBudget_TTrace_1784623364 ----

INVARIANT
    _inv

CHECK_DEADLOCK
    \* CHECK_DEADLOCK off because of PROPERTY or INVARIANT above.
    FALSE

INIT
    _init

NEXT
    _next

CONSTANT
    _TETrace <- _trace

ALIAS
    _expression
=============================================================================
\* Generated on Tue Jul 21 10:42:44 CEST 2026