// SPDX-License-Identifier: Apache-2.0

// Package viz provides visualization tools for Raft execution traces.
package viz

import (
	"fmt"
	"html/template"
	"io"
	"sort"
	"time"
)

// Event represents a single event in the timeline.
type Event struct {
	Time        int64  // Virtual time in milliseconds
	Type        string // "election", "append", "commit", "heartbeat", "vote"
	NodeID      string
	Target      string // Target node (for messages)
	Term        uint64
	Description string
	Success     bool
	Data        map[string]interface{} // Additional data
}

// NodeState captures node state at a point in time.
type NodeState struct {
	Time        int64
	NodeID      string
	Role        string // "follower", "candidate", "leader"
	Term        uint64
	VotedFor    string
	CommitIndex uint64
	LogLength   int
}

// Timeline holds all events and states for visualization.
type Timeline struct {
	Events     []Event
	States     []NodeState
	StartTime  int64
	EndTime    int64
	Nodes      []string
	Violations []Violation
}

// Violation represents an invariant violation.
type Violation struct {
	Time        int64
	Name        string
	Description string
	Severity    string
}

// NewTimeline creates a new timeline.
func NewTimeline(nodes []string) *Timeline {
	return &Timeline{
		Events: make([]Event, 0),
		States: make([]NodeState, 0),
		Nodes:  nodes,
	}
}

// AddEvent adds an event to the timeline.
func (t *Timeline) AddEvent(e Event) {
	t.Events = append(t.Events, e)
	if t.StartTime == 0 || e.Time < t.StartTime {
		t.StartTime = e.Time
	}
	if e.Time > t.EndTime {
		t.EndTime = e.Time
	}
}

// AddState adds a node state snapshot.
func (t *Timeline) AddState(s NodeState) {
	t.States = append(t.States, s)
}

// AddViolation adds an invariant violation.
func (t *Timeline) AddViolation(v Violation) {
	t.Violations = append(t.Violations, v)
}

// Sort sorts all events and states by time.
func (t *Timeline) Sort() {
	sort.Slice(t.Events, func(i, j int) bool {
		return t.Events[i].Time < t.Events[j].Time
	})
	sort.Slice(t.States, func(i, j int) bool {
		return t.States[i].Time < t.States[i].Time
	})
	sort.Slice(t.Violations, func(i, j int) bool {
		return t.Violations[i].Time < t.Violations[j].Time
	})
}

// GenerateHTML generates an interactive HTML timeline visualization.
func (t *Timeline) GenerateHTML(w io.Writer, title string) error {
	t.Sort()

	data := struct {
		Title      string
		Nodes      []string
		Events     []Event
		States     []NodeState
		Violations []Violation
		Duration   int64
		StartTime  int64
		EventCount int
	}{
		Title:      title,
		Nodes:      t.Nodes,
		Events:     t.Events,
		States:     t.States,
		Violations: t.Violations,
		Duration:   t.EndTime - t.StartTime,
		StartTime:  t.StartTime,
		EventCount: len(t.Events),
	}

	tmpl, err := template.New("timeline").Funcs(template.FuncMap{
		"formatTime": func(ms int64) string {
			return fmt.Sprintf("%.3fs", float64(ms)/1000.0)
		},
		"formatDuration": func(ms int64) string {
			d := time.Duration(ms) * time.Millisecond
			return d.String()
		},
		"div": func(a, b int64) int64 {
			if b == 0 {
				return 0
			}
			return a / b
		},
		"mul": func(a, b int64) int64 {
			return a * b
		},
		"sub": func(a, b int64) int64 {
			return a - b
		},
		"eventColor": func(eventType string) string {
			colors := map[string]string{
				"election":  "#3498db",
				"vote":      "#2ecc71",
				"append":    "#f39c12",
				"commit":    "#27ae60",
				"heartbeat": "#95a5a6",
				"message":   "#34495e",
			}
			if color, ok := colors[eventType]; ok {
				return color
			}
			return "#7f8c8d"
		},
		"roleColor": func(role string) string {
			colors := map[string]string{
				"leader":    "#e74c3c",
				"candidate": "#f39c12",
				"follower":  "#3498db",
			}
			if color, ok := colors[role]; ok {
				return color
			}
			return "#95a5a6"
		},
	}).Parse(htmlTemplate)

	if err != nil {
		return fmt.Errorf("failed to parse template: %w", err)
	}

	if err := tmpl.Execute(w, data); err != nil {
		return fmt.Errorf("failed to execute template: %w", err)
	}

	return nil
}

const htmlTemplate = `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{{.Title}} - Raft Timeline</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: #f5f7fa;
            color: #2c3e50;
            padding: 20px;
        }
        .header {
            background: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            margin-bottom: 20px;
        }
        .header h1 { margin-bottom: 10px; color: #2c3e50; }
        .stats {
            display: flex;
            gap: 30px;
            color: #7f8c8d;
            font-size: 14px;
        }
        .stat { display: flex; align-items: center; gap: 8px; }
        .stat-value { font-weight: 600; color: #2c3e50; }
        
        .timeline-container {
            background: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            overflow-x: auto;
        }
        
        .nodes {
            display: grid;
            grid-template-columns: 100px 1fr;
            gap: 10px;
            margin-bottom: 20px;
        }
        
        .node-label {
            font-weight: 600;
            padding: 8px;
            background: #ecf0f1;
            border-radius: 4px;
            display: flex;
            align-items: center;
        }
        
        .node-timeline {
            position: relative;
            height: 40px;
            background: #f8f9fa;
            border-radius: 4px;
        }
        
        .event {
            position: absolute;
            width: 8px;
            height: 8px;
            border-radius: 50%;
            transform: translate(-50%, -50%);
            top: 50%;
            cursor: pointer;
            transition: all 0.2s;
        }
        
        .event:hover {
            transform: translate(-50%, -50%) scale(1.5);
            z-index: 10;
        }
        
        .event-tooltip {
            position: absolute;
            bottom: 100%;
            left: 50%;
            transform: translateX(-50%);
            background: rgba(0,0,0,0.9);
            color: white;
            padding: 8px 12px;
            border-radius: 4px;
            font-size: 12px;
            white-space: nowrap;
            display: none;
            z-index: 100;
            margin-bottom: 5px;
        }
        
        .event:hover .event-tooltip {
            display: block;
        }
        
        .events-list {
            margin-top: 30px;
            max-height: 400px;
            overflow-y: auto;
        }
        
        .event-item {
            padding: 10px;
            border-left: 3px solid #3498db;
            margin-bottom: 8px;
            background: #f8f9fa;
            border-radius: 0 4px 4px 0;
            font-size: 14px;
        }
        
        .event-time {
            color: #7f8c8d;
            font-size: 12px;
            margin-bottom: 4px;
        }
        
        .violations {
            margin-top: 20px;
            padding: 15px;
            background: #ffe5e5;
            border-left: 4px solid #e74c3c;
            border-radius: 4px;
        }
        
        .violations h3 {
            color: #e74c3c;
            margin-bottom: 10px;
        }
        
        .violation-item {
            padding: 8px;
            background: white;
            border-radius: 4px;
            margin-bottom: 8px;
        }
        
        .success { color: #27ae60; }
        .failure { color: #e74c3c; }
        
        .legend {
            display: flex;
            gap: 20px;
            margin-top: 20px;
            padding: 15px;
            background: #f8f9fa;
            border-radius: 4px;
        }
        
        .legend-item {
            display: flex;
            align-items: center;
            gap: 8px;
            font-size: 13px;
        }
        
        .legend-color {
            width: 12px;
            height: 12px;
            border-radius: 50%;
        }
    </style>
</head>
<body>
    <div class="header">
        <h1>{{.Title}}</h1>
        <div class="stats">
            <div class="stat">
                <span>Duration:</span>
                <span class="stat-value">{{formatDuration .Duration}}</span>
            </div>
            <div class="stat">
                <span>Events:</span>
                <span class="stat-value">{{.EventCount}}</span>
            </div>
            <div class="stat">
                <span>Nodes:</span>
                <span class="stat-value">{{len .Nodes}}</span>
            </div>
            {{if .Violations}}
            <div class="stat">
                <span class="failure">⚠ Violations:</span>
                <span class="stat-value failure">{{len .Violations}}</span>
            </div>
            {{end}}
        </div>
    </div>

    {{if .Violations}}
    <div class="violations">
        <h3>⚠ Invariant Violations Detected</h3>
        {{range .Violations}}
        <div class="violation-item">
            <div class="event-time">{{formatTime .Time}}</div>
            <div><strong>{{.Name}}:</strong> {{.Description}}</div>
        </div>
        {{end}}
    </div>
    {{end}}

    <div class="timeline-container">
        <h2 style="margin-bottom: 15px;">Node Timeline</h2>
        
        <div class="nodes">
            {{range $nodeID := .Nodes}}
            <div class="node-label">{{$nodeID}}</div>
            <div class="node-timeline" id="timeline-{{$nodeID}}">
                {{range $.Events}}
                {{if eq .NodeID $nodeID}}
                <div class="event" 
                     style="left: {{if gt $.Duration 0}}{{div (mul (sub .Time $.StartTime) 100) $.Duration}}%{{else}}0%{{end}}; 
                            background: {{eventColor .Type}};">
                    <div class="event-tooltip">
                        {{formatTime .Time}} - {{.Type}}<br>
                        {{if .Target}}→ {{.Target}}<br>{{end}}
                        {{.Description}}
                    </div>
                </div>
                {{end}}
                {{end}}
            </div>
            {{end}}
        </div>

        <div class="legend">
            <div class="legend-item">
                <div class="legend-color" style="background: {{eventColor "election"}};"></div>
                <span>Election</span>
            </div>
            <div class="legend-item">
                <div class="legend-color" style="background: {{eventColor "vote"}};"></div>
                <span>Vote</span>
            </div>
            <div class="legend-item">
                <div class="legend-color" style="background: {{eventColor "append"}};"></div>
                <span>Append</span>
            </div>
            <div class="legend-item">
                <div class="legend-color" style="background: {{eventColor "commit"}};"></div>
                <span>Commit</span>
            </div>
            <div class="legend-item">
                <div class="legend-color" style="background: {{eventColor "heartbeat"}};"></div>
                <span>Heartbeat</span>
            </div>
        </div>

        <div class="events-list">
            <h3 style="margin-bottom: 10px;">Event Log</h3>
            {{range .Events}}
            <div class="event-item" style="border-color: {{eventColor .Type}};">
                <div class="event-time">{{formatTime .Time}}</div>
                <div>
                    <strong>[{{.NodeID}}{{if .Target}} → {{.Target}}{{end}}]</strong> 
                    {{.Type}} (term {{.Term}})
                    {{if .Success}}<span class="success">✓</span>{{else}}<span class="failure">✗</span>{{end}}
                </div>
                {{if .Description}}
                <div style="color: #7f8c8d; font-size: 12px; margin-top: 4px;">{{.Description}}</div>
                {{end}}
            </div>
            {{end}}
        </div>
    </div>

    <script>
        // Add keyboard shortcuts
        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape') {
                document.querySelectorAll('.event-tooltip').forEach(t => t.style.display = 'none');
            }
        });
        
        // Click to highlight related events
        document.querySelectorAll('.event').forEach(event => {
            event.addEventListener('click', function() {
                document.querySelectorAll('.event').forEach(e => e.style.opacity = '0.3');
                this.style.opacity = '1';
            });
        });
        
        // Double-click to reset
        document.addEventListener('dblclick', () => {
            document.querySelectorAll('.event').forEach(e => e.style.opacity = '1');
        });
    </script>
</body>
</html>`
