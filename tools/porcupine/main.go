// Check a Keel history with somebody else's linearizability checker.
//
// The simulator's oracles are ours. We chose the properties, we wrote the
// checks, and a property nobody thought of is a property nobody checks.
// Porcupine applies a definition of linearizability written by someone with no
// stake in whether Keel is correct, to a history Keel's own client recorded, and
// it does not care what anybody believes about the code.
//
// The half that makes the other half evidence is -mutate. A checker that accepts
// everything also accepts a correct history, so accepting one proves nothing on
// its own. With -mutate, one read's returned value is changed to something no
// write ever produced, and the checker must reject it. Both runs are recorded.
//
// Usage:
//
//	porcupine -history history.jsonl
//	porcupine -history history.jsonl -mutate -out mutated.jsonl
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/anishathalye/porcupine"
)

// One line of Keel's history. The field names are the ones
// keel-client/src/history.rs writes; a mismatch here is silent data loss, so
// the parser rejects a line it cannot make sense of rather than skipping it.
type entry struct {
	Op          string          `json:"op"`
	Key         string          `json:"key"`
	Value       json.RawMessage `json:"value"`
	InvokedUs   int64           `json:"invoked_us"`
	CompletedUs *int64          `json:"completed_us"`
	Outcome     string          `json:"outcome"`
	Result      json.RawMessage `json:"result"`
}

type input struct {
	op    string
	value string
}

// A register per key. The empty string is "absent", which a get of a key nobody
// has written must return.
var registerModel = porcupine.Model{
	Init: func() interface{} { return "" },
	Step: func(state, in, out interface{}) (bool, interface{}) {
		st := state.(string)
		i := in.(input)
		if i.op == "put" {
			return true, i.value
		}
		// A read is the only thing that can contradict the model, which is why
		// the history has to record what reads returned.
		return out.(string) == st, st
	},
	DescribeOperation: func(in, out interface{}) string {
		i := in.(input)
		if i.op == "put" {
			return fmt.Sprintf("put(%s)", i.value)
		}
		return fmt.Sprintf("get() -> %q", out)
	},
}

func unquote(raw json.RawMessage) (string, bool) {
	if len(raw) == 0 || string(raw) == "null" {
		return "", false
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return "", false
	}
	return s, true
}

func load(path string) ([]entry, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var entries []entry
	scanner := bufio.NewScanner(f)
	// A history from a long run is a large file, and a value is arbitrary
	// bytes rendered as hex, so a line can be much longer than the default.
	scanner.Buffer(make([]byte, 1024*1024), 16*1024*1024)
	line := 0
	for scanner.Scan() {
		line++
		text := scanner.Bytes()
		if len(text) == 0 {
			continue
		}
		var e entry
		if err := json.Unmarshal(text, &e); err != nil {
			return nil, fmt.Errorf("line %d: %w", line, err)
		}
		entries = append(entries, e)
	}
	return entries, scanner.Err()
}

// Turn history entries into operations the checker can use, grouped by key.
//
// What is dropped, and why each one is safe to drop:
//
//   - anything that is not a get or a put. The model is a register; a scan or a
//     counter is a different model and mixing them would check neither.
//   - a refused operation. The cluster said no, definitely, so it did not
//     happen and constrains nothing.
//   - an unanswered *read*. We never learned what it returned, so there is no
//     value for the model to contradict.
//   - an operation still pending when the history was taken.
//
// What is emphatically *not* dropped is an unanswered write. It may or may not
// have applied, and dropping it would let a later read observe a value the
// checker believes was never written — a correct history rejected. It is given
// a return time past the end of the history instead, which is porcupine's way
// of saying "this could have taken effect at any point after it was called".
func operations(entries []entry) (map[string][]porcupine.Operation, int, int) {
	latest := int64(0)
	for _, e := range entries {
		if e.CompletedUs != nil && *e.CompletedUs > latest {
			latest = *e.CompletedUs
		}
		if e.InvokedUs > latest {
			latest = e.InvokedUs
		}
	}
	// Comfortably past everything, and still nowhere near overflowing.
	never := latest + int64(time.Hour/time.Microsecond)

	byKey := make(map[string][]porcupine.Operation)
	kept, dropped := 0, 0
	for _, e := range entries {
		if e.Op != "get" && e.Op != "put" {
			dropped++
			continue
		}
		switch e.Outcome {
		case "refused", "pending":
			dropped++
			continue
		case "unknown":
			if e.Op == "get" {
				dropped++
				continue
			}
		}

		var in input
		var out string
		switch e.Op {
		case "put":
			v, ok := unquote(e.Value)
			if !ok {
				dropped++
				continue
			}
			in = input{op: "put", value: v}
		case "get":
			// A result of null is a key that was absent, which the model
			// represents as the empty string. That is only a correct reading
			// for an outcome of "ok" — which is why unanswered reads were
			// dropped above rather than being given a null result here.
			v, _ := unquote(e.Result)
			in = input{op: "get"}
			out = v
		}

		ret := never
		if e.Outcome == "ok" && e.CompletedUs != nil {
			ret = *e.CompletedUs
		}
		byKey[e.Key] = append(byKey[e.Key], porcupine.Operation{
			Input:  in,
			Call:   e.InvokedUs,
			Output: out,
			Return: ret,
		})
		kept++
	}
	return byKey, kept, dropped
}

// Change one read's returned value to something no write in the history ever
// produced, so a checker that is doing its job has to reject it.
//
// The key chosen is the one with the most operations, because a mutation on a
// busy key is the hardest to detect: there are more orderings for a broken
// result to hide in.
//
// The *position* on that key used to be the last such read, on the same
// reasoning, and it stopped working the day the recorded history gained real
// concurrency. Refuting a history is not the same problem as accepting one: to
// accept, a checker finds one linearization and stops; to refute, it must
// exhaust the search space and show there is none. With the mutation at the end
// of a thirteen-thousand-operation partition, exhausting it meant linearizing
// everything before it, and the control timed out with "the search did not
// finish".
//
// A control that cannot finish is not a control. It reports neither a pass nor
// a failure, and an arm that reports nothing cannot make the other arm evidence.
// So the mutation goes a quarter of the way into the busy key instead: still
// surrounded by the full concurrency of the run — every client has its whole
// pipeline in flight there — and reachable within a bounded search.
//
// What this arm claims is therefore precise, and narrower than it was: this
// checker, on this data, tells a corrupted read from a real one. It does not
// claim the checker would catch a corruption anywhere in a history of any size.
const mutateAt = 4 // one part in four

func mutate(entries []entry) (int, error) {
	counts := make(map[string]int)
	for _, e := range entries {
		counts[e.Key]++
	}
	best, bestCount := "", -1
	for key, n := range counts {
		if n > bestCount {
			best, bestCount = key, n
		}
	}

	eligible := func(e *entry) bool {
		if e.Key != best || e.Op != "get" || e.Outcome != "ok" {
			return false
		}
		_, ok := unquote(e.Result)
		return ok
	}

	// Which eligible read to take: a quarter of the way through the ones this
	// key has, rather than the first, so there is a real prefix of writes for
	// the corrupted value to contradict.
	total := 0
	for i := range entries {
		if eligible(&entries[i]) {
			total++
		}
	}
	if total == 0 {
		return 0, fmt.Errorf("no completed read with a value to mutate; the history is not usable as a control")
	}
	target, seen := total/mutateAt, 0
	for i := range entries {
		if !eligible(&entries[i]) {
			continue
		}
		if seen < target {
			seen++
			continue
		}
		// Deliberately not any value in the history: a value that was written
		// somewhere might linearize, and then the mutated history would be
		// accepted for a legitimate reason and the demonstration would prove
		// the opposite of what it claims.
		entries[i].Result = json.RawMessage(`"deadbeefdeadbeef"`)
		return i, nil
	}
	return 0, fmt.Errorf("no completed read with a value to mutate; the history is not usable as a control")
}

func main() {
	historyPath := flag.String("history", "", "the JSONL history to check")
	doMutate := flag.Bool("mutate", false, "corrupt one read's result before checking; the checker must then reject")
	out := flag.String("out", "", "with -mutate, where to write the corrupted history")
	timeout := flag.Duration("timeout", 60*time.Second, "give up after this long")
	limit := flag.Int("limit", 0, "check only the first N operations; 0 means all")
	flag.Parse()

	if *historyPath == "" {
		fmt.Fprintln(os.Stderr, "-history is required")
		os.Exit(2)
	}
	entries, err := load(*historyPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "reading %s: %v\n", *historyPath, err)
		os.Exit(2)
	}
	fmt.Printf("history:   %s, %d entries\n", *historyPath, len(entries))

	// A bounded prefix, for the control arm.
	//
	// Refuting a history and accepting one are not the same problem. To accept,
	// the checker finds one linearization and stops. To refute, it must exhaust
	// the space and show there is none — and on a history recorded at depth
	// eight, exhausting it took more memory than the machine had. The control
	// was killed by the kernel partway through the first key, which reports
	// neither a pass nor a failure, and an arm that reports nothing cannot make
	// the other arm evidence.
	//
	// So the control checks a prefix and says how long it is. The experiment
	// arm still checks everything: accepting is the cheap direction, and it is
	// the direction the real claim is in.
	if *limit > 0 && *limit < len(entries) {
		entries = entries[:*limit]
		fmt.Printf("limited:   the first %d operations\n", len(entries))
	}

	mutatedAt := -1
	if *doMutate {
		mutatedAt, err = mutate(entries)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		fmt.Printf("mutated:   entry %d, key %s, result replaced with a value nothing wrote\n",
			mutatedAt, entries[mutatedAt].Key)
		if *out != "" {
			f, err := os.Create(*out)
			if err != nil {
				fmt.Fprintf(os.Stderr, "writing %s: %v\n", *out, err)
				os.Exit(2)
			}
			enc := json.NewEncoder(f)
			for _, e := range entries {
				if err := enc.Encode(e); err != nil {
					fmt.Fprintf(os.Stderr, "writing %s: %v\n", *out, err)
					os.Exit(2)
				}
			}
			f.Close()
		}
	}

	byKey, kept, dropped := operations(entries)
	fmt.Printf("checked:   %d operations over %d keys (%d not applicable to a register model)\n",
		kept, len(byKey), dropped)
	if kept == 0 {
		fmt.Fprintln(os.Stderr, "nothing to check: a history with no operations is not evidence")
		os.Exit(2)
	}

	keys := make([]string, 0, len(byKey))
	for key := range byKey {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	worst := porcupine.Ok
	for _, key := range keys {
		ops := byKey[key]
		result := porcupine.CheckOperationsTimeout(registerModel, ops, *timeout)
		fmt.Printf("  key %-16s %4d ops   %s\n", key, len(ops), verdict(result))
		if result == porcupine.Illegal {
			worst = porcupine.Illegal
		} else if result == porcupine.Unknown && worst != porcupine.Illegal {
			worst = porcupine.Unknown
		}
	}

	fmt.Println()
	switch {
	case *doMutate && worst == porcupine.Illegal:
		fmt.Println("PASS the checker rejected the corrupted history, so its acceptance means something")
	case *doMutate:
		fmt.Printf("FAIL the checker accepted a history with a fabricated read (%s)\n", verdict(worst))
		os.Exit(1)
	case worst == porcupine.Ok:
		fmt.Println("PASS the history is linearizable")
	default:
		fmt.Printf("FAIL %s\n", verdict(worst))
		os.Exit(1)
	}
}

func verdict(r porcupine.CheckResult) string {
	switch r {
	case porcupine.Ok:
		return "linearizable"
	case porcupine.Illegal:
		return "NOT linearizable"
	default:
		// A timeout is not a pass. Porcupine's search is exponential in the
		// worst case, and "we ran out of time" answers a different question
		// from "it is correct".
		return "unknown (the search did not finish)"
	}
}
