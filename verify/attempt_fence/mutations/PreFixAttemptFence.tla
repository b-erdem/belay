------------------------ MODULE PreFixAttemptFence ------------------------
EXTENDS Naturals, FiniteSets

Attempts == {1, 2}
States == {"ready", "running", "succeeded"}

VARIABLES state, currentAttempt, activeAttempts, committedBy
vars == <<state, currentAttempt, activeAttempts, committedBy>>

Init ==
  /\ state = "ready"
  /\ currentAttempt = 0
  /\ activeAttempts = {}
  /\ committedBy = 0

Claim ==
  /\ state = "ready"
  /\ currentAttempt < 2
  /\ state' = "running"
  /\ currentAttempt' = currentAttempt + 1
  /\ activeAttempts' = activeAttempts \cup {currentAttempt + 1}
  /\ UNCHANGED committedBy

Expire(a) ==
  /\ state = "running"
  /\ a = currentAttempt
  /\ a \in activeAttempts
  /\ state' = "ready"
  /\ UNCHANGED <<currentAttempt, activeAttempts, committedBy>>

\* MUTATION: missing a = currentAttempt. A zombie can commit over attempt 2.
Ack(a) ==
  /\ state = "running"
  /\ a \in activeAttempts
  /\ state' = "succeeded"
  /\ committedBy' = a
  /\ activeAttempts' = activeAttempts \ {a}
  /\ UNCHANGED currentAttempt

RejectStaleAck(a) ==
  /\ a \in activeAttempts
  /\ a # currentAttempt
  /\ activeAttempts' = activeAttempts \ {a}
  /\ UNCHANGED <<state, currentAttempt, committedBy>>

Done ==
  /\ state = "succeeded"
  /\ UNCHANGED vars

Exhausted ==
  /\ state = "ready"
  /\ currentAttempt = 2
  /\ UNCHANGED vars

Next ==
  \/ Claim
  \/ \E a \in Attempts : Expire(a) \/ Ack(a) \/ RejectStaleAck(a)
  \/ Done
  \/ Exhausted

Spec == Init /\ [][Next]_vars

TypeOK ==
  /\ state \in States
  /\ currentAttempt \in 0..2
  /\ activeAttempts \subseteq Attempts
  /\ committedBy \in 0..2

StaleAttemptCannotCommit == committedBy = 0 \/ committedBy = currentAttempt
=============================================================================
