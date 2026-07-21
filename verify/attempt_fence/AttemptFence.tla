---------------------------- MODULE AttemptFence ----------------------------
EXTENDS Naturals, FiniteSets

(***************************************************************************)
(* A focused model of Belay's attempt fence. An expired worker may stay  *)
(* alive after the lease is reclaimed. Its eventual ack must not overwrite  *)
(* the replacement attempt that now owns the row.                           *)
(***************************************************************************)

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

Ack(a) ==
  /\ state = "running"
  /\ a \in activeAttempts
  /\ a = currentAttempt
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
