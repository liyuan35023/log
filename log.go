//high level log wrapper, so it can output different log based on level
package log
import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"regexp"
	"runtime"
	"runtime/debug"
	"github.com/liyuan35023/log/gcontext"
	"sort"
	"strings"
	"sync"
	"time"
	"io/ioutil"
)
const (
	Ldate         = log.Ldate
	Llongfile     = log.Llongfile
	Lmicroseconds = log.Lmicroseconds
	Lshortfile    = log.Lshortfile
	LstdFlags     = log.LstdFlags
	Ltime         = log.Ltime
)
type (
	LogLevel int
	LogType  int
)
const (
	LOG_FATAL   = LogType(0x1)
	LOG_ERROR   = LogType(0x2)
	LOG_WARNING = LogType(0x4)
	LOG_INFO    = LogType(0x8)
	LOG_DEBUG   = LogType(0x10)
)
type RollPeriod bool
const (
	ROLL_DAY   RollPeriod = true
	ROLL_HOURE RollPeriod = false
)
const (
	LOG_LEVEL_NONE  = LogLevel(0x0)
	LOG_LEVEL_FATAL = LOG_LEVEL_NONE | LogLevel(LOG_FATAL)
	LOG_LEVEL_ERROR = LOG_LEVEL_FATAL | LogLevel(LOG_ERROR)
	LOG_LEVEL_WARN  = LOG_LEVEL_ERROR | LogLevel(LOG_WARNING)
	LOG_LEVEL_INFO  = LOG_LEVEL_WARN | LogLevel(LOG_INFO)
	LOG_LEVEL_DEBUG = LOG_LEVEL_INFO | LogLevel(LOG_DEBUG)
	LOG_LEVEL_ALL   = LOG_LEVEL_DEBUG
	LOG_LEVEL_DEBUG_STRING = "debug"
	LOG_LEVEL_INFO_STRING  = "info"
	LOG_LEVEL_ERROR_STRING = "error"
	LOG_LEVEL_FATAL_STRING = "faltal"
)
var logLevelMap = map[string]LogLevel{
	LOG_LEVEL_DEBUG_STRING: LOG_LEVEL_DEBUG,
	LOG_LEVEL_INFO_STRING:  LOG_LEVEL_INFO,
	LOG_LEVEL_ERROR_STRING: LOG_LEVEL_ERROR,
	LOG_LEVEL_FATAL_STRING: LOG_LEVEL_FATAL,
}
func ConvertLogLevelString(level string) LogLevel {
	if val, ok := logLevelMap[level]; ok {
		return val
	}
	return LOG_LEVEL_ALL
}
const FORMAT_TIME_DAY string = "20060102"
const FORMAT_TIME_HOUR string = "2006010215"
var _log *logger = NewConsoleLog()
type SohuCSLogConf struct {
	LogFlags     int
	Level        LogLevel
	Highlighting bool
	RollPeriod   RollPeriod
	FileName     string
	Prefix       string
	MaxLogs      int
}
func Init(cfg *SohuCSLogConf) error {
	setHighlighting(cfg.Highlighting)
	setFlags(cfg.LogFlags)
	if cfg.RollPeriod {
		setRotateByDay()
	} else {
		setRotateByHour()
	}
	if cfg == nil {
		return errors.New("input cfg point is nil")
	}
	err := setOutputByName(cfg.FileName)
	if err != nil {
		return err
	}
	setLevel(cfg.Level)
	setPreFix(cfg.Prefix)
	setMaxLogs(cfg.MaxLogs)
	return nil
}
func init() {
	setFlags(Ldate | Ltime | Lshortfile)
	setHighlighting(runtime.GOOS != "windows")
}
func setLevel(level LogLevel) {
	_log.SetLevel(level)
}
func GetLogLevel() LogLevel {
	return _log.level
}
func setPreFix(prefix string) {
	_log.setPrefix(prefix)
}
func setMaxLogs(maxLogs int) {
	_log.SetMaxLogs(maxLogs)
}
func setOutputByName(path string) error {
	return _log.SetOutputByName(path)
}
func setFlags(flags int) {
	_log._log.SetFlags(flags)
}
func getSuffix() string {
	return _log.logSuffix
}
func Info(v ...interface{}) {
	_log.Info(v...)
}
func Infoc(ctx context.Context, v ...interface{}) {
	_log.Infoc(ctx, v...)
}
func Infof(format string, v ...interface{}) {
	_log.Infof(format, v...)
}
func Infocf(ctx context.Context, format string, v ...interface{}) {
	_log.Infocf(ctx, format, v...)
}
func Debug(v ...interface{}) {
	_log.Debug(v...)
}
func Debugc(ctx context.Context, v ...interface{}) {
	_log.Debugc(ctx, v...)
}
func Debugf(format string, v ...interface{}) {
	_log.Debugf(format, v...)
}
func Debugcf(ctx context.Context, format string, v ...interface{}) {
	_log.Debugcf(ctx, format, v...)
}
func Warn(v ...interface{}) {
	_log.Warning(v...)
}
func Warnc(ctx context.Context, v ...interface{}) {
	_log.Warningc(ctx, v...)
}
func Warnf(format string, v ...interface{}) {
	_log.Warningf(format, v...)
}
func Warncf(ctx context.Context, format string, v ...interface{}) {
	_log.Warningcf(ctx, format, v...)
}
func Warning(v ...interface{}) {
	_log.Warning(v...)
}
func Warningf(format string, v ...interface{}) {
	_log.Warningf(format, v...)
}
func Error(v ...interface{}) {
	v = append(v, GetCallStack(1))
	_log.Error(v...)
}
func Errorc(ctx context.Context, v ...interface{}) {
	v = append(v, GetCallStack(1))
	_log.Errorc(ctx, v...)
}
func Errorf(format string, v ...interface{}) {
	v = append(v, GetCallStack(1))
	_log.Errorf(format+"%s", v...)
}
func Errorcf(ctx context.Context, format string, v ...interface{}) {
	v = append(v, GetCallStack(1))
	_log.Errorcf(ctx, format+"%s", v...)
}
func Fatal(v ...interface{}) {
	v = append(v, GetCallStack(1))
	_log.Fatal(v...)
}
func Fatalc(ctx context.Context, v ...interface{}) {
	v = append(v, GetCallStack(1))
	_log.Fatalc(ctx, v...)
}
func Fatalf(format string, v ...interface{}) {
	v = append(v, GetCallStack(1))
	_log.Fatalf(format+"%s", v...)
}
func Fatalcf(ctx context.Context, format string, v ...interface{}) {
	v = append(v, GetCallStack(1))
	_log.Fatalcf(ctx, format+"%s", v...)
}
func PrintfWithDepth(callDepth int, format string, v ...interface{}) {
	_log.PrintfWithDepth(callDepth, format, v...)
}
func Printf(format string, v ...interface{}) {
	_log.Infof(format, v...)
}
func setHighlighting(highlighting bool) {
	_log.SetHighlighting(highlighting)
}
func setRotateByDay() {
	_log.SetRotateByDay()
}
func setRotateByHour() {
	_log.SetRotateByHour()
}
func GetLogger() PrintfLogger {
	return _log
}
// sort log file by createTime embedded in the filename
type byCreatedTime []string
func (ct byCreatedTime) Len() int {
	return len(ct)
}
func (ct byCreatedTime) Less(i, j int) bool {
	ct1 := ct[i]
	ct2 := ct[j]
	t1, err := time.Parse(FORMAT_TIME_DAY, ct1)
	if err != nil {
		t1, _ = time.Parse(FORMAT_TIME_HOUR, ct1)
	}
	t2, err := time.Parse(FORMAT_TIME_DAY, ct2)
	if err != nil {
		t2, _ = time.Parse(FORMAT_TIME_HOUR, ct2)
	}
	return t1.Before(t2)
}
func (ct byCreatedTime) Swap(i, j int) {
	ct[i], ct[j] = ct[j], ct[i]
}
func getLogsFromPath(dir string, logName string) ([]string, error) {
	var fileNames []string
	fileInfos, err := ioutil.ReadDir(dir)
	if err != nil {
		return fileNames, err
	}
	// regex
	pattern := fmt.Sprintf("^%s.(\\d+)$", logName)
	re := regexp.MustCompile(pattern)
	for _, df := range fileInfos {
		if isMatched := re.MatchString(df.Name()); isMatched {
			fileTime := re.FindStringSubmatch(df.Name())[1]
			fileNames = append(fileNames, fileTime)
		}
	}
	return fileNames, nil
}
type PrintfLogger interface {
	Printf(format string, v ...interface{})
}
type logger struct {
	_log         *log.Logger
	level        LogLevel
	highlighting bool
	dailyRolling bool
	hourRolling  bool
	fileName  string
	logSuffix string
	fd        *os.File
	maxLogs int
	lock sync.Mutex
}
func (l *logger) SetHighlighting(highlighting bool) {
	l.highlighting = highlighting
}
func (l *logger) SetLevel(level LogLevel) {
	l.level = level
}
func (l *logger) SetLevelByString(level string) {
	l.level = StringToLogLevel(level)
}
func (l *logger) SetRotateByDay() {
	l.dailyRolling = true
	l.logSuffix = genDayTime(time.Now())
}
func (l *logger) SetRotateByHour() {
	l.hourRolling = true
	l.logSuffix = genHourTime(time.Now())
}
func (l *logger) SetMaxLogs(maxLogs int) {
	l.maxLogs = maxLogs
}
func (l *logger) rotate() error {
	l.lock.Lock()
	defer l.lock.Unlock()
	var suffix string
	if l.dailyRolling {
		suffix = genDayTime(time.Now())
	} else if l.hourRolling {
		suffix = genHourTime(time.Now())
	} else {
		return nil
	}
	// Notice: if suffix is not equal to l.LogSuffix, then rotate
	if suffix != l.logSuffix {
		err := l.doRotate(suffix)
		if err != nil {
			return err
		}
	}
	return nil
}
func (l *logger) doRotate(suffix string) error {
	// Notice: Not check error, is this ok?
	l.fd.Close()
	lastFileName := l.fileName + "." + l.logSuffix
	err := os.Rename(l.fileName, lastFileName)
	if err != nil {
		return err
	}
	err = l.SetOutputByName(l.fileName)
	if err != nil {
		return err
	}
	l.logSuffix = suffix
	go l.deleteLogs()
	return nil
}
func (l *logger) SetOutput(out io.Writer) {
	l._log = log.New(out, l._log.Prefix(), l._log.Flags())
}
func (l *logger) SetOutputByName(path string) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0666)
	if err != nil {
		log.Fatal(err)
	}
	l.SetOutput(f)
	l.fileName = path
	l.fd = f
	return err
}
func (l *logger) deleteLogs() {
	// default to keep all logs
	if l.maxLogs <= 0 {
		return
	}
	tmp := strings.Split(l.fileName, "/")
	actualName := tmp[len(tmp)-1]
	logPath := strings.TrimSuffix(l.fileName, "/"+actualName)
	logs, err := getLogsFromPath(logPath, actualName)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err.Error())
		return
	}
	if len(logs)+1 > l.maxLogs {
		err = l.doDelete(logs, actualName, logPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s\n", err.Error())
			return
		}
	}
}
func (l *logger) doDelete(logs []string, actualName, logPath string) error {
	sort.Sort(byCreatedTime(logs))
	needToDelete := (len(logs) + 1) - l.maxLogs
	for i := 0; i < needToDelete; i++ {
		filePath := logPath + "/" + actualName + "." + logs[i]
		err := os.Remove(filePath)
		if err != nil {
			return err
		}
	}
	return nil
}
func (l *logger) log(ctx context.Context, t LogType, v ...interface{}) {
	if l.level|LogLevel(t) != l.level {
		return
	}
	err := l.rotate()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err.Error())
		return
	}
	traceId := ""
	if ctx != nil {
		traceId = " [TRACEID:" + gcontext.GetTraceIdFromMetadata(ctx) + "]"
	}
	v1 := make([]interface{}, len(v)+2)
	logStr, logColor := LogTypeToString(t)
	if l.highlighting {
		v1[0] = "\033" + logColor + "m[" + logStr + "]" + traceId
		copy(v1[1:], v)
		v1[len(v)+1] = "\033[0m"
	} else {
		v1[0] = "[" + logStr + "]" + traceId
		copy(v1[1:], v)
		v1[len(v)+1] = ""
	}
	s := fmt.Sprintln(v1...)
	l._log.Output(4, s)
}
func (l *logger) logText(ctx context.Context, t LogType, format string, v ...interface{}) string {
	err := l.rotate()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err.Error())
		return ""
	}
	logStr, logColor := LogTypeToString(t)
	var s string
	if l.highlighting {
		s = "\033" + logColor + "m[" + logStr + "] " + l.logFormatText(ctx, format, v...) + "\033[0m"
	} else {
		s = "[" + logStr + "] " + l.logFormatText(ctx, format, v...)
	}
	return s
}
func (l *logger) logf(ctx context.Context, t LogType, format string, v ...interface{}) {
	if l.level|LogLevel(t) != l.level {
		return
	}
	s := l.logText(ctx, t, format, v...)
	l.output(5, s)
}
func (l *logger) logfWithDepth(ctx context.Context, callDepth int, t LogType, format string, v ...interface{}) {
	s := l.logText(ctx, t, format, v...)
	l.output(callDepth, s)
}
func (l *logger) output(calldepth int, s string) {
	l._log.Output(calldepth, s)
}
func (l *logger) logFormatText(ctx context.Context, format string, v ...interface{}) string {
	var s string
	if ctx == nil {
		s = fmt.Sprintf(format, v...)
	} else {
		s = fmt.Sprintf("[TRACEID:"+gcontext.GetTraceIdFromMetadata(ctx)+"]"+format, v...)
	}
	return s
}
func (l *logger) PrintfWithDepth(callDepth int, format string, v ...interface{}) {
	l.logfWithDepth(nil, callDepth+5, LOG_INFO, format, v...)
}
func (l *logger) Fatal(v ...interface{}) {
	l.log(nil, LOG_FATAL, v...)
	os.Exit(-1)
}
func (l *logger) Fatalc(ctx context.Context, v ...interface{}) {
	l.log(ctx, LOG_FATAL, v...)
	os.Exit(-1)
}
func (l *logger) Fatalf(format string, v ...interface{}) {
	l.logf(nil, LOG_FATAL, format, v...)
	os.Exit(-1)
}
func (l *logger) Fatalcf(ctx context.Context, format string, v ...interface{}) {
	l.logf(ctx, LOG_FATAL, format, v...)
	os.Exit(-1)
}
func (l *logger) Error(v ...interface{}) {
	l.log(nil, LOG_ERROR, v...)
}
func (l *logger) Errorc(ctx context.Context, v ...interface{}) {
	l.log(ctx, LOG_ERROR, v...)
}
func (l *logger) Errorf(format string, v ...interface{}) {
	l.logf(nil, LOG_ERROR, format, v...)
}
func (l *logger) Errorcf(ctx context.Context, format string, v ...interface{}) {
	l.logf(ctx, LOG_ERROR, format, v...)
}
func (l *logger) Warning(v ...interface{}) {
	l.log(nil, LOG_WARNING, v...)
}
func (l *logger) Warningc(ctx context.Context, v ...interface{}) {
	l.log(ctx, LOG_WARNING, v...)
}
func (l *logger) Warningf(format string, v ...interface{}) {
	l.logf(nil, LOG_WARNING, format, v...)
}
func (l *logger) Warningcf(ctx context.Context, format string, v ...interface{}) {
	l.logf(ctx, LOG_WARNING, format, v...)
}
func (l *logger) Debug(v ...interface{}) {
	l.log(nil, LOG_DEBUG, v...)
}
func (l *logger) Debugc(ctx context.Context, v ...interface{}) {
	l.log(ctx, LOG_DEBUG, v...)
}
func (l *logger) Debugf(format string, v ...interface{}) {
	l.logf(nil, LOG_DEBUG, format, v...)
}
func (l *logger) Debugcf(ctx context.Context, format string, v ...interface{}) {
	l.logf(ctx, LOG_DEBUG, format, v...)
}
func (l *logger) Info(v ...interface{}) {
	l.log(nil, LOG_INFO, v...)
}
func (l *logger) Infoc(ctx context.Context, v ...interface{}) {
	l.log(ctx, LOG_INFO, v...)
}
func (l *logger) Infof(format string, v ...interface{}) {
	l.logf(nil, LOG_INFO, format, v...)
}
func (l *logger) Printf(format string, v ...interface{}) {
	l.logf(nil, LOG_INFO, format, v...)
}
func (l *logger) Infocf(ctx context.Context, format string, v ...interface{}) {
	l.logf(ctx, LOG_INFO, format, v...)
}
func (l *logger) setPrefix(prefix string) {
	l._log.SetPrefix(prefix)
}
func StringToLogLevel(level string) LogLevel {
	switch level {
	case "fatal":
		return LOG_LEVEL_FATAL
	case "error":
		return LOG_LEVEL_ERROR
	case "warn":
		return LOG_LEVEL_WARN
	case "warning":
		return LOG_LEVEL_WARN
	case "debug":
		return LOG_LEVEL_DEBUG
	case "info":
		return LOG_LEVEL_INFO
	}
	return LOG_LEVEL_ALL
}
func LogTypeToString(t LogType) (string, string) {
	switch t {
	case LOG_FATAL:
		return "fatal", "[0;31"
	case LOG_ERROR:
		return "error", "[0;31"
	case LOG_WARNING:
		return "warning", "[0;33"
	case LOG_DEBUG:
		return "debug", "[0;36"
	case LOG_INFO:
		return "info", "[0;37"
	}
	return "unknown", "[0;37"
}
func genDayTime(t time.Time) string {
	return t.Format(FORMAT_TIME_DAY)
}
func genHourTime(t time.Time) string {
	return t.Format(FORMAT_TIME_HOUR)
}
func NewConsoleLog() *logger {
	return Newlogger(os.Stdout, "")
}
func Newlogger(w io.Writer, prefix string) *logger {
	return &logger{_log: log.New(w, prefix, LstdFlags), level: LOG_LEVEL_ALL, highlighting: true}
}
func GetCallStack(calldepth int) string {
	callstatck := string(debug.Stack())
	splitStack := strings.Split(callstatck, "\n")
	var i int
	codeStack := "\n" + splitStack[0]
	for i = (calldepth+2)*2 + 1; i < len(splitStack); i++ {
		codeStack += ("\n" + splitStack[i])
	}
	return codeStack
}
