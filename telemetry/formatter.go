package telemetry

import (
	"fmt"
	"regexp"
	"strings"
	"time"
)

// handles formatting of telemetry data
// :date | :level | :levelpad | :message | :metadata | :node | :time
type Pattern string

// metadata holds specific fields of interest for the pattern
type PatternMetadata struct {
	Date     time.Time
	Level    string
	Message  string
	Event    string
	Node     string
	Time     time.Time
	Metadata map[string]interface{}
}

// build the pattern's specific data field generated from the metadata fields
type PatternFunc func(metadata *PatternMetadata)

const (
	Date     Pattern = "date"
	Level    Pattern = "level"
	Message  Pattern = "message"
	Metadata Pattern = "metadata"
	Event    Pattern = "event"
	Node     Pattern = "node"
	Time     Pattern = "time"
)

type Formatter struct {
	// takes a string a returns an array of patterns
	PatternList []string
}

// build the formatter interface
type FormatterBuilder interface {
	CompileString(pattern string)
	CompileJson(pattern []string)
	FormatString(patternData ...PatternFunc) string
	// FormatJson(patternData ...PatternFunc) string
}

// create a new formatter
func NewFormatter() FormatterBuilder {
	return &Formatter{}
}

// CompileString takes the format string and splits it into tokens and literal segments.
// For instance, for the input "$date $time [$level] - event: $event - node: $node - $metadata $message"
// it generates the slice:
// [$date, " ", $time, " [", $level, "] - event: ", $event, " - node: ", $node, " - ", $metadata, " ", $message]
func (f *Formatter) CompileString(pattern string) {
	// Regex to match tokens that start with '$' followed by alphanumeric characters or underscores
	tokenRegex := regexp.MustCompile(`\$[a-zA-Z0-9_]+`)
	matches := tokenRegex.FindAllStringIndex(pattern, -1)

	var tokens []string
	lastIndex := 0

	for _, match := range matches {
		start, end := match[0], match[1]
		// If there is a literal text before the token, add it.
		if start > lastIndex {
			literal := pattern[lastIndex:start]
			// In case you want to split literal parts further you can use strings.Split or leave as is.
			tokens = append(tokens, literal)
		}
		// Add the token.
		tokens = append(tokens, pattern[start:end])
		lastIndex = end
	}

	// Add any remaining literal text after the last token.
	if lastIndex < len(pattern) {
		tokens = append(tokens, pattern[lastIndex:])
	}

	f.PatternList = tokens
}

func (f *Formatter) CompileJson(pattern []string) {
	f.PatternList = pattern
}

// format the given pattern
// replace
func (f *Formatter) FormatString(patternFuncs ...PatternFunc) string {
	// Build pattern data
	patternData := &PatternMetadata{}
	for _, patternFunc := range patternFuncs {
		patternFunc(patternData)
	}

	var result strings.Builder
	for _, token := range f.PatternList {
		if strings.HasPrefix(token, "$") {
			// Replace token with its name (without the '$' prefix)
			result.WriteString(getPatternData(patternData, token[1:]))
		} else {
			// Literal text, add it unchanged
			result.WriteString(token)
		}
	}
	return result.String()
}

// get the data
func getPatternData(p *PatternMetadata, tokenName string) string {
	switch tokenName {
	case "date":
		return p.Date.Format("2006-01-02")
	case "time":
		return p.Time.Format("15:04:05")
	case "level":
		return p.Level
	case "event":
		return p.Event
	case "node":
		return p.Node
	case "metadata":
		return formatMetadata(p.Metadata)
	case "message":
		return p.Message
	default:
		return ""
	}
}

func formatMetadata(metadata map[string]interface{}) string {
	if len(metadata) == 0 {
		return ""
	}
	var pairs []string
	for key, value := range metadata {
		pairs = append(pairs, fmt.Sprintf("%s:%v", key, value))
	}
	return strings.Join(pairs, ",")
}

// default pattern func
func SetDate(d time.Time) PatternFunc {
	return func(metadata *PatternMetadata) {
		metadata.Date = d // date.Format("2006-01-02")
	}
}

func SetTime(t time.Time) PatternFunc {
	return func(metadata *PatternMetadata) {
		metadata.Time = t // time.Format("15:04:05")
	}
}

func SetLevel(level LogLevel) PatternFunc {
	return func(metadata *PatternMetadata) {
		metadata.Level = string(level)
	}
}

func SetMessage(message string) PatternFunc {
	return func(metadata *PatternMetadata) {
		metadata.Message = message
	}
}

func SetEvent(event TelemetryEvent) PatternFunc {
	return func(metadata *PatternMetadata) {
		metadata.Event = string(event)
	}
}

func SetNode(node string) PatternFunc {
	return func(metadata *PatternMetadata) {
		metadata.Node = node
	}
}

func SetMetadata(meta map[string]interface{}) PatternFunc {
	return func(metadata *PatternMetadata) {
		metadata.Metadata = meta
	}
}
