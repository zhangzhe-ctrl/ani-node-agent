package gpu

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	cri "k8s.io/cri-api/pkg/apis/runtime/v1"
)

type JournalPodRef struct {
	UID, Namespace, Name, ContainerName string
}

type JournalConfig struct {
	Directory      string        `json:"directory"`
	MachineID      string        `json:"machine_id"`
	NodeUID        string        `json:"node_uid"`
	JournalctlPath string        `json:"journalctl_path"`
	TimeoutSeconds int           `json:"timeout_seconds,omitempty"`
	Timeout        time.Duration `json:"-"`
	cursor         string
}

const journalMaxOutput = 4 << 20
const journalMaxAttempts = 16

var (
	journalHex32  = regexp.MustCompile(`^[a-f0-9]{32}$`)
	journalUID    = regexp.MustCompile(`^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$`)
	journalName   = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]{0,251}[a-z0-9]$|^[a-z0-9]$`)
	journalMsg    = regexp.MustCompile(`(?:^| )msg=("(?:[^"\\]|\\.)*")`)
	journalPod    = regexp.MustCompile(`^RunPodSandbox for name:"([^"]+)"\s+uid:"([^"]+)"\s+namespace:"([^"]+)"(?:\s+attempt:[0-9]+)?( returns sandbox id "([a-f0-9]{64})")?$`)
	journalMake   = regexp.MustCompile(`^CreateContainer within sandbox "([a-f0-9]{64})" for (?:container )?name:"([^"]+)"(?:\s+attempt:[0-9]+)?( returns container id "([a-f0-9]{64})")?$`)
	journalExit   = regexp.MustCompile(`^received container exit event container_id:"([a-f0-9]{64})"\s+id:"([a-f0-9]{64})"(?:\s+pid:[0-9]+)?(?:\s+exit_status:[0-9]+)?\s+exited_at:\{seconds:([0-9]+)(?:\s+nanos:([0-9]+))?\}$`)
	journalStart  = regexp.MustCompile(`^StartContainer for "([a-f0-9]{64})"(?: returns successfully)?$`)
	journalCursor = regexp.MustCompile(`^s=([a-f0-9]{32});i=([a-f0-9]+);b=([a-f0-9]{32});m=([a-f0-9]+);t=([a-f0-9]+);x=([a-f0-9]+)$`)
)

type journalRecord struct {
	Message, Machine, Boot, Unit, Comm, UID, Exe, Transport, Cursor string
	Realtime, Monotonic                                             int64
}

type journalQueryFunc func(context.Context, JournalConfig, string) ([]journalRecord, error)

func RecoverJournalExit(ctx context.Context, cfg JournalConfig, pod JournalPodRef) ([]*cri.ContainerStatusResponse, bool, error) {
	return recoverJournalExitWith(ctx, cfg, pod, readJournalRecords)
}

func recoverJournalExitWith(ctx context.Context, cfg JournalConfig, pod JournalPodRef, query journalQueryFunc) ([]*cri.ContainerStatusResponse, bool, error) {
	if !journalHex32.MatchString(cfg.MachineID) || cfg.NodeUID == "" || !journalUID.MatchString(pod.UID) || !journalName.MatchString(pod.Namespace) || !journalName.MatchString(pod.Name) || !journalName.MatchString(pod.ContainerName) || !filepath.IsAbs(cfg.Directory) || filepath.Clean(cfg.Directory) != cfg.Directory || !filepath.IsAbs(cfg.JournalctlPath) || filepath.Base(cfg.JournalctlPath) != "journalctl" {
		return nil, false, errors.New("runtime journal configuration or scope invalid")
	}
	ctx, cancel := context.WithTimeout(ctx, journalTimeout(cfg))
	defer cancel()
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	read := func(grep string) ([]journalRecord, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		records, err := query(ctx, cfg, grep)
		if err != nil {
			return nil, err
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		for _, record := range records {
			if record.Machine != cfg.MachineID || !journalHex32.MatchString(record.Boot) || record.Unit != "containerd.service" || record.Comm != "containerd" || record.UID != "0" || !filepath.IsAbs(record.Exe) || filepath.Base(record.Exe) != "containerd" || record.Transport != "stdout" || record.Cursor == "" || record.Realtime <= 0 || record.Monotonic <= 0 {
				return nil, errors.New("runtime journal source identity invalid")
			}
		}
		return records, nil
	}
	records, err := read(pod.UID)
	if err != nil {
		return nil, false, err
	}
	sandboxes := map[string]journalRecord{}
	var podRecords []journalRecord
	starts, returns := 0, 0
	for _, record := range records {
		match := journalPod.FindStringSubmatch(record.Message)
		if match == nil {
			if strings.HasPrefix(record.Message, "RunPodSandbox for ") && strings.Contains(record.Message, pod.UID) {
				return nil, false, nil
			}
			continue
		}
		if match[2] != pod.UID {
			continue
		}
		if match[1] != pod.Name || match[3] != pod.Namespace {
			return nil, false, nil
		}
		podRecords = append(podRecords, record)
		if match[5] == "" {
			starts++
			continue
		}
		returns++
		if previous, ok := sandboxes[match[5]]; ok && (previous.Boot != record.Boot || previous.Monotonic != record.Monotonic) {
			return nil, false, nil
		}
		sandboxes[match[5]] = record
	}
	if len(sandboxes) == 0 || len(sandboxes) > journalMaxAttempts || starts > returns {
		return nil, false, nil
	}
	cfg.cursor = journalRecoveryCursor(podRecords)
	records, err = read(journalKeys(sandboxes))
	if err != nil {
		return nil, false, err
	}
	creates := map[string]journalRecord{}
	containerSandboxes := map[string]string{}
	requests, successes := map[string]int{}, map[string]int{}
	for _, record := range records {
		match := journalMake.FindStringSubmatch(record.Message)
		if match == nil {
			if strings.HasPrefix(record.Message, "CreateContainer within sandbox ") {
				for id := range sandboxes {
					if strings.Contains(record.Message, id) {
						return nil, false, nil
					}
				}
			}
			continue
		}
		if match[2] != pod.ContainerName {
			continue
		}
		sandbox, ok := sandboxes[match[1]]
		if !ok {
			continue
		}
		if sandbox.Boot != record.Boot || record.Monotonic < sandbox.Monotonic {
			return nil, false, nil
		}
		if match[4] == "" {
			requests[match[1]]++
			continue
		}
		successes[match[1]]++
		if previous, ok := creates[match[4]]; ok && (previous.Boot != record.Boot || previous.Monotonic != record.Monotonic || containerSandboxes[match[4]] != match[1]) {
			return nil, false, nil
		}
		creates[match[4]], containerSandboxes[match[4]] = record, match[1]
	}
	for sandbox := range sandboxes {
		if requests[sandbox] > successes[sandbox] {
			return nil, false, nil
		}
	}
	if len(creates) == 0 || len(creates) > journalMaxAttempts {
		return nil, false, nil
	}
	records, err = read(journalKeys(creates))
	if err != nil {
		return nil, false, err
	}
	exits := map[string]journalRecord{}
	finished := map[string]int64{}
	lastStarts := map[string]int64{}
	for _, record := range records {
		if match := journalStart.FindStringSubmatch(record.Message); match != nil {
			if create, ok := creates[match[1]]; ok {
				if record.Boot != create.Boot {
					return nil, false, nil
				}
				lastStarts[match[1]] = max(lastStarts[match[1]], record.Monotonic)
			}
		}
		match := journalExit.FindStringSubmatch(record.Message)
		if match == nil {
			if strings.HasPrefix(record.Message, "received container exit event ") {
				for id := range creates {
					if strings.Contains(record.Message, id) {
						return nil, false, nil
					}
				}
			}
			continue
		}
		create, ok := creates[match[1]]
		if !ok {
			continue
		}
		seconds, e1 := strconv.ParseInt(match[3], 10, 64)
		nanos := int64(0)
		var e2 error
		if match[4] != "" {
			nanos, e2 = strconv.ParseInt(match[4], 10, 64)
		}
		if e1 != nil || e2 != nil || seconds <= 0 || seconds > (1<<63-1)/int64(time.Second) || nanos < 0 || nanos >= int64(time.Second) || match[1] != match[2] || record.Boot != create.Boot || record.Monotonic < create.Monotonic {
			return nil, false, nil
		}
		at := time.Unix(seconds, nanos).UnixNano()
		if at <= 0 || at > record.Realtime*1000+999 || at > time.Now().UnixNano() {
			return nil, false, nil
		}
		if previous, ok := finished[match[1]]; ok && previous != at {
			return nil, false, nil
		}
		finished[match[1]], exits[match[1]] = at, record
	}
	var result []*cri.ContainerStatusResponse
	for _, id := range strings.Split(journalKeys(creates), "|") {
		create := creates[id]
		exit, ok := exits[id]
		if !ok || lastStarts[id] > exit.Monotonic {
			return nil, false, nil
		}
		sandbox := sandboxes[containerSandboxes[id]]
		provenance, _ := json.Marshal(map[string]any{"machine_id": cfg.MachineID, "boot_id": exit.Boot, "node_uid": cfg.NodeUID, "pod_uid": pod.UID, "sandbox_id": containerSandboxes[id], "container_id": id, "sandbox_cursor": sandbox.Cursor, "create_cursor": create.Cursor, "exit_cursor": exit.Cursor, "finished_at": finished[id], "created_at": create.Realtime * 1000, "sandbox_monotonic": sandbox.Monotonic, "create_monotonic": create.Monotonic, "exit_monotonic": exit.Monotonic, "exe": exit.Exe})
		digest := sha256.Sum256(provenance)
		result = append(result, &cri.ContainerStatusResponse{Status: &cri.ContainerStatus{Id: id, Metadata: &cri.ContainerMetadata{Name: pod.ContainerName}, Labels: map[string]string{"io.kubernetes.pod.uid": pod.UID, "io.kubernetes.pod.namespace": pod.Namespace, "io.kubernetes.pod.name": pod.Name}, State: cri.ContainerState_CONTAINER_EXITED, FinishedAt: finished[id]}, Info: map[string]string{"acc.runtime_journal": string(provenance), "journal_evidence": "journal:" + hex.EncodeToString(digest[:])}})
	}
	return result, true, nil
}

func journalKeys(records map[string]journalRecord) string {
	keys := make([]string, 0, len(records))
	for id := range records {
		keys = append(keys, id)
	}
	sort.Strings(keys)
	return strings.Join(keys, "|")
}

func journalTimeout(cfg JournalConfig) time.Duration {
	if cfg.Timeout > 0 {
		return min(cfg.Timeout, 25*time.Second)
	}
	if cfg.TimeoutSeconds > 0 {
		return time.Duration(min(cfg.TimeoutSeconds, 25)) * time.Second
	}
	return 20 * time.Second
}

func journalArguments(cfg JournalConfig, grep string) []string {
	args := []string{"--directory=" + cfg.Directory, "--unit=containerd.service", "--no-pager", "--output=json", "--all", "--quiet", "--grep=" + grep}
	if cfg.cursor != "" {
		args = append(args, "--cursor="+cfg.cursor)
	}
	return append(args, "_MACHINE_ID="+cfg.MachineID)
}

func journalRecoveryCursor(records []journalRecord) string {
	if len(records) == 0 {
		return ""
	}
	earliest := records[0]
	for _, record := range records[1:] {
		if record.Boot != earliest.Boot {
			return ""
		}
		if record.Monotonic < earliest.Monotonic {
			earliest = record
		}
	}
	match := journalCursor.FindStringSubmatch(earliest.Cursor)
	if match == nil || match[3] != earliest.Boot {
		return ""
	}
	monotonic, e1 := strconv.ParseInt(match[4], 16, 64)
	realtime, e2 := strconv.ParseInt(match[5], 16, 64)
	if e1 != nil || e2 != nil || monotonic != earliest.Monotonic || realtime != earliest.Realtime {
		return ""
	}
	return earliest.Cursor
}

func readJournalRecords(ctx context.Context, cfg JournalConfig, grep string) ([]journalRecord, error) {
	info, err := os.Lstat(cfg.Directory)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("runtime journal original directory unavailable")
	}
	command := exec.CommandContext(ctx, cfg.JournalctlPath, journalArguments(cfg, grep)...)
	command.WaitDelay = time.Second
	command.Env = append(os.Environ(), "LC_ALL=C", "SYSTEMD_COLORS=0", "SYSTEMD_PAGER=cat")
	output := &journalBoundedBuffer{limit: journalMaxOutput}
	command.Stdout, command.Stderr = output, io.Discard
	if err = command.Run(); err != nil || output.overflow {
		return nil, errors.New("runtime journal query unavailable or exceeded bound")
	}
	return decodeJournalRecords(output.Bytes())
}

type journalBoundedBuffer struct {
	buffer   bytes.Buffer
	limit    int
	overflow bool
}

func (output *journalBoundedBuffer) Bytes() []byte  { return output.buffer.Bytes() }
func (output *journalBoundedBuffer) String() string { return output.buffer.String() }

func (output *journalBoundedBuffer) Write(data []byte) (int, error) {
	remaining := output.limit - output.buffer.Len()
	if len(data) > remaining {
		output.overflow = true
		written, _ := output.buffer.Write(data[:remaining])
		return written, io.ErrShortBuffer
	}
	return output.buffer.Write(data)
}

func decodeJournalRecords(data []byte) ([]journalRecord, error) {
	if len(data) > journalMaxOutput {
		return nil, errors.New("runtime journal output exceeded bound")
	}
	scanner := bufio.NewScanner(bytes.NewReader(data))
	scanner.Buffer(make([]byte, 4096), 64<<10)
	var records []journalRecord
	for scanner.Scan() {
		if len(records) >= 4096 {
			return nil, errors.New("runtime journal record limit exceeded")
		}
		var raw map[string]string
		if err := json.Unmarshal(scanner.Bytes(), &raw); err != nil {
			return nil, errors.New("runtime journal record invalid")
		}
		matches := journalMsg.FindAllStringSubmatch(raw["MESSAGE"], -1)
		if len(matches) != 1 {
			return nil, errors.New("runtime journal message invalid")
		}
		message, e1 := strconv.Unquote(matches[0][1])
		realtime, e2 := strconv.ParseInt(raw["__REALTIME_TIMESTAMP"], 10, 64)
		monotonic, e3 := strconv.ParseInt(raw["__MONOTONIC_TIMESTAMP"], 10, 64)
		if e1 != nil || e2 != nil || e3 != nil || realtime > (1<<63-1)/1000 {
			return nil, errors.New("runtime journal message or timestamp invalid")
		}
		records = append(records, journalRecord{Message: message, Machine: raw["_MACHINE_ID"], Boot: raw["_BOOT_ID"], Unit: raw["_SYSTEMD_UNIT"], Comm: raw["_COMM"], UID: raw["_UID"], Exe: raw["_EXE"], Transport: raw["_TRANSPORT"], Cursor: raw["__CURSOR"], Realtime: realtime, Monotonic: monotonic})
	}
	if err := scanner.Err(); err != nil {
		return nil, errors.New("runtime journal record exceeded bound")
	}
	return records, nil
}
