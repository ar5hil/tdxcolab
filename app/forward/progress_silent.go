package forward

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/fatih/color"
	"github.com/mattn/go-runewidth"

	"github.com/iyear/tdl/core/forwarder"
	"github.com/iyear/tdl/pkg/utils"
)

type silentProgress struct {
	mu       sync.Mutex
	states   map[tuple]silentProgressState
	elemName map[int64]string
}

type silentProgressState struct {
	start time.Time
	clone bool
	done  int64
	total int64
}

func newSilentProgress() *silentProgress {
	return &silentProgress{
		states:   make(map[tuple]silentProgressState),
		elemName: make(map[int64]string),
	}
}

func (p *silentProgress) OnAdd(elem forwarder.Elem) {
	p.mu.Lock()
	p.states[p.tuple(elem)] = silentProgressState{
		start: time.Now(),
	}
	msg := p.processMessage(elem, false)
	p.mu.Unlock()

	fmt.Printf("%s ... forwarding ...\n", msg)
}

func (p *silentProgress) OnClone(elem forwarder.Elem, state forwarder.ProgressState) {
	p.mu.Lock()
	defer p.mu.Unlock()

	t := p.tuple(elem)
	progressState, ok := p.states[t]
	if !ok {
		return
	}

	progressState.clone = true
	progressState.done = state.Done
	progressState.total = state.Total
	p.states[t] = progressState
}

func (p *silentProgress) OnDone(elem forwarder.Elem, err error) {
	p.mu.Lock()
	t := p.tuple(elem)
	progressState, ok := p.states[t]
	if ok {
		delete(p.states, t)
	}
	msg := p.processMessage(elem, ok && progressState.clone)
	p.mu.Unlock()

	if err != nil {
		fmt.Println(color.RedString("%s error: %s", msg, err.Error()))
		return
	}

	fmt.Printf("%s ... done! [%s]\n", msg, p.summary(progressState))
}

func (p *silentProgress) summary(state silentProgressState) string {
	elapsed := time.Since(state.start)
	if elapsed <= 0 {
		elapsed = 10 * time.Millisecond
	}

	// use 10ms precision to keep output concise and stable across platforms.
	elapsed = elapsed.Round(10 * time.Millisecond)
	if elapsed <= 0 {
		elapsed = 10 * time.Millisecond
	}

	seconds := elapsed.Seconds()
	if seconds <= 0 {
		seconds = 0.01
	}

	if state.clone {
		done := state.done
		if done <= 0 {
			done = state.total
		}
		if done < 0 {
			done = 0
		}

		speed := int64(float64(done) / seconds)
		return fmt.Sprintf("%s in %s; %s/s",
			utils.Byte.FormatBinaryBytes(done),
			elapsed,
			utils.Byte.FormatBinaryBytes(speed))
	}

	rate := float64(1) / seconds
	return fmt.Sprintf("1 in %s; %.0f/s", elapsed, rate)
}

func (p *silentProgress) tuple(elem forwarder.Elem) tuple {
	return tuple{
		from: elem.From().ID(),
		msg:  elem.Msg().ID,
		to:   elem.To().ID(),
	}
}

func (p *silentProgress) processMessage(elem forwarder.Elem, clone bool) string {
	b := &strings.Builder{}

	b.WriteString(p.metaString(elem))
	if clone {
		b.WriteString(" [clone]")
	}

	return b.String()
}

func (p *silentProgress) metaString(elem forwarder.Elem) string {
	if _, ok := p.elemName[elem.From().ID()]; !ok {
		p.elemName[elem.From().ID()] = runewidth.Truncate(elem.From().VisibleName(), 15, "...")
	}
	if _, ok := p.elemName[elem.To().ID()]; !ok {
		p.elemName[elem.To().ID()] = runewidth.Truncate(elem.To().VisibleName(), 15, "...")
	}

	return fmt.Sprintf("%s(%d):%d -> %s(%d)",
		p.elemName[elem.From().ID()],
		elem.From().ID(),
		elem.Msg().ID,
		p.elemName[elem.To().ID()],
		elem.To().ID())
}
