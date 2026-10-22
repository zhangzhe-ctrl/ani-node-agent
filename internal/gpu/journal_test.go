package gpu

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	cri "k8s.io/cri-api/pkg/apis/runtime/v1"
)

const journalTestMachine = "2e5cc7ce6b9c4666a1943a6fa65c1858"
const journalTestBoot = "6644a471edb642618328f5a5a02fc2f0"

func journalFixture() (JournalConfig, JournalPodRef, [][]byte) {
	cfg := JournalConfig{Directory: "/host/journal/" + journalTestMachine, MachineID: journalTestMachine, NodeUID: "a59bc8aa-8f31-41a7-a402-4c88d0211dac", JournalctlPath: "/usr/bin/journalctl"}
	pod := JournalPodRef{Namespace: "tenant", Name: "instance-0", UID: "481c72aa-da10-4544-ba08-29f090cf6e4d", ContainerName: "workspace"}
	sandbox, container := strings.Repeat("d", 64), strings.Repeat("1", 64)
	lines := [][]byte{
		journalLine(`RunPodSandbox for name:"instance-0" uid:"`+pod.UID+`" namespace:"tenant" returns sandbox id "`+sandbox+`"`, 100),
		journalLine(`CreateContainer within sandbox "`+sandbox+`" for name:"workspace" returns container id "`+container+`"`, 200),
		journalLine(`received container exit event container_id:"`+container+`" id:"`+container+`" pid:2672423 exited_at:{seconds:1600000000 nanos:424252708}`, 300),
	}
	return cfg, pod, lines
}

func journalLine(message string, monotonic int) []byte {
	record := map[string]string{
		"MESSAGE":     `time="2020-09-13T12:26:41Z" level=info msg=` + strconv.Quote(message),
		"_MACHINE_ID": journalTestMachine, "_BOOT_ID": journalTestBoot,
		"_SYSTEMD_UNIT": "containerd.service", "_COMM": "containerd", "_UID": "0",
		"_EXE": "/usr/local/bin/containerd", "_TRANSPORT": "stdout",
		"__REALTIME_TIMESTAMP": "1600000001000000", "__MONOTONIC_TIMESTAMP": strconv.Itoa(monotonic),
		"__CURSOR": "cursor-" + strconv.Itoa(monotonic),
	}
	line, _ := json.Marshal(record)
	return line
}

func journalQuery(lines [][]byte) journalQueryFunc {
	return func(_ context.Context, _ JournalConfig, _ string) ([]journalRecord, error) {
		return decodeJournalRecords(bytes.Join(lines, []byte("\n")))
	}
}

func TestJournalRecoveryUsesCompleteOriginalIdentityAndPositiveExit(testContext *testing.T) {
	cfg, pod, lines := journalFixture()
	statuses, ok, err := recoverJournalExitWith(context.Background(), cfg, pod, journalQuery(lines))
	if err != nil || !ok || len(statuses) != 1 {
		testContext.Fatalf("complete journal chain: statuses=%+v ok=%v error=%v", statuses, ok, err)
	}
	status := statuses[0].Status
	if status.Id != strings.Repeat("1", 64) || status.Metadata.Name != pod.ContainerName || status.Labels["io.kubernetes.pod.uid"] != pod.UID || status.Labels["io.kubernetes.pod.namespace"] != pod.Namespace || status.Labels["io.kubernetes.pod.name"] != pod.Name || status.State != cri.ContainerState_CONTAINER_EXITED || status.FinishedAt != 1600000000424252708 {
		testContext.Fatalf("identity or positive exit changed: %+v", status)
	}
	for _, value := range []string{cfg.NodeUID, journalTestMachine, journalTestBoot, "cursor-100", "cursor-200", "cursor-300"} {
		if !strings.Contains(statuses[0].Info["acc.runtime_journal"], value) {
			testContext.Fatalf("missing original journal provenance %q: %+v", value, statuses[0].Info)
		}
	}
}

func TestJournalRecoveryRejectsIncompleteOrConflictingFacts(testContext *testing.T) {
	for _, test := range []struct {
		name string
		edit func(*JournalConfig, *JournalPodRef, *[][]byte)
	}{
		{"missing_sandbox", func(_ *JournalConfig, _ *JournalPodRef, lines *[][]byte) { *lines = (*lines)[1:] }},
		{"missing_create", func(_ *JournalConfig, _ *JournalPodRef, lines *[][]byte) { *lines = append((*lines)[:1], (*lines)[2]) }},
		{"missing_exit", func(_ *JournalConfig, _ *JournalPodRef, lines *[][]byte) { *lines = (*lines)[:2] }},
		{"different_pod_uid", func(_ *JournalConfig, pod *JournalPodRef, _ *[][]byte) {
			pod.UID = "9a306469-bb48-47d6-9e50-4190e0602a49"
		}},
		{"different_namespace", func(_ *JournalConfig, pod *JournalPodRef, _ *[][]byte) { pod.Namespace = "another" }},
		{"different_name", func(_ *JournalConfig, pod *JournalPodRef, _ *[][]byte) { pod.Name = "replacement-0" }},
		{"different_container", func(_ *JournalConfig, pod *JournalPodRef, _ *[][]byte) { pod.ContainerName = "sidecar" }},
		{"wrong_machine", func(config *JournalConfig, _ *JournalPodRef, _ *[][]byte) { config.MachineID = strings.Repeat("a", 32) }},
		{"another_boot_exit", func(_ *JournalConfig, _ *JournalPodRef, lines *[][]byte) {
			(*lines)[2] = []byte(strings.ReplaceAll(string((*lines)[2]), journalTestBoot, strings.Repeat("a", 32)))
		}},
		{"untrusted_process", func(_ *JournalConfig, _ *JournalPodRef, lines *[][]byte) {
			(*lines)[2] = []byte(strings.ReplaceAll(string((*lines)[2]), `"_UID":"0"`, `"_UID":"1000"`))
		}},
		{"wrong_unit", func(_ *JournalConfig, _ *JournalPodRef, lines *[][]byte) {
			(*lines)[2] = []byte(strings.ReplaceAll(string((*lines)[2]), "containerd.service", "application.service"))
		}},
		{"exit_event_id_conflict", func(_ *JournalConfig, _ *JournalPodRef, lines *[][]byte) {
			(*lines)[2] = journalLine(`received container exit event container_id:"`+strings.Repeat("1", 64)+`" id:"`+strings.Repeat("2", 64)+`" exited_at:{seconds:1600000000 nanos:424252708}`, 300)
		}},
		{"exit_before_create", func(_ *JournalConfig, _ *JournalPodRef, lines *[][]byte) {
			(*lines)[2] = journalLine(`received container exit event container_id:"`+strings.Repeat("1", 64)+`" id:"`+strings.Repeat("1", 64)+`" exited_at:{seconds:1600000000 nanos:424252708}`, 150)
		}},
		{"unknown_attempt", func(_ *JournalConfig, _ *JournalPodRef, lines *[][]byte) {
			*lines = append(*lines, journalLine(`CreateContainer within sandbox "`+strings.Repeat("d", 64)+`" for name:"workspace" attempt:1 returns container id "`+strings.Repeat("2", 64)+`"`, 400))
		}},
		{"new_start_after_exit", func(_ *JournalConfig, _ *JournalPodRef, lines *[][]byte) {
			*lines = append(*lines, journalLine(`StartContainer for "`+strings.Repeat("1", 64)+`"`, 400))
		}},
		{"no_finished_at", func(_ *JournalConfig, _ *JournalPodRef, lines *[][]byte) {
			(*lines)[2] = journalLine(`received container exit event container_id:"`+strings.Repeat("1", 64)+`" id:"`+strings.Repeat("1", 64)+`"`, 300)
		}},
		{"malformed_timestamp", func(_ *JournalConfig, _ *JournalPodRef, lines *[][]byte) {
			(*lines)[2] = journalLine(`received container exit event container_id:"`+strings.Repeat("1", 64)+`" id:"`+strings.Repeat("1", 64)+`" exited_at:{seconds:1600000000 nanos:1000000000}`, 300)
		}},
		{"not_found_only", func(_ *JournalConfig, _ *JournalPodRef, lines *[][]byte) {
			(*lines)[2] = journalLine(`ContainerStatus for "`+strings.Repeat("1", 64)+`" failed: NotFound`, 300)
		}},
		{"remove_only", func(_ *JournalConfig, _ *JournalPodRef, lines *[][]byte) {
			(*lines)[2] = journalLine(`RemoveContainer for "`+strings.Repeat("1", 64)+`" returns successfully`, 300)
		}},
		{"state_text_only", func(_ *JournalConfig, _ *JournalPodRef, lines *[][]byte) {
			(*lines)[2] = journalLine(`Container to stop "`+strings.Repeat("1", 64)+`" must be in running or unknown state, current state "CONTAINER_EXITED"`, 300)
		}},
		{"invalid_json", func(_ *JournalConfig, _ *JournalPodRef, lines *[][]byte) { (*lines)[2] = []byte(`{"MESSAGE":`) }},
	} {
		testContext.Run(test.name, func(testContext *testing.T) {
			cfg, pod, lines := journalFixture()
			test.edit(&cfg, &pod, &lines)
			statuses, ok, _ := recoverJournalExitWith(context.Background(), cfg, pod, journalQuery(lines))
			if ok || len(statuses) != 0 {
				testContext.Fatalf("incomplete or conflicting original records released occupancy: %+v", statuses)
			}
		})
	}
}

func TestJournalRecoveryRequiresEveryAttemptAndKeepsIdentity(testContext *testing.T) {
	cfg, pod, lines := journalFixture()
	second := strings.Repeat("2", 64)
	lines = append(lines, journalLine(`CreateContainer within sandbox "`+strings.Repeat("d", 64)+`" for name:"workspace" attempt:1 returns container id "`+second+`"`, 400), journalLine(`received container exit event container_id:"`+second+`" id:"`+second+`" exit_status:137 exited_at:{seconds:1600000000 nanos:800000000}`, 500))
	statuses, ok, err := recoverJournalExitWith(context.Background(), cfg, pod, journalQuery(lines))
	if err != nil || !ok || len(statuses) != 2 || statuses[0].Status.Id != strings.Repeat("1", 64) || statuses[1].Status.Id != second {
		testContext.Fatalf("all attempts not preserved: %+v %v %v", statuses, ok, err)
	}
	if reflect.DeepEqual(statuses[0].Status, statuses[1].Status) {
		testContext.Fatal("attempt identity collapsed")
	}
}

func TestJournalRecoveryStopsOnQueryError(testContext *testing.T) {
	cfg, pod, _ := journalFixture()
	statuses, ok, err := recoverJournalExitWith(context.Background(), cfg, pod, func(context.Context, JournalConfig, string) ([]journalRecord, error) {
		return nil, errors.New("unreadable journal")
	})
	if err == nil || ok || len(statuses) != 0 {
		testContext.Fatalf("unavailable journal became positive: %+v %v %v", statuses, ok, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, ok, err := recoverJournalExitWith(ctx, cfg, pod, journalQuery(nil)); ok || err == nil {
		testContext.Fatal("cancelled collection became positive")
	}
}

func TestJournalCommandArgumentsAndOutputAreBounded(testContext *testing.T) {
	cfg, _, _ := journalFixture()
	got := journalArguments(cfg, "a|b")
	want := []string{"--directory=" + cfg.Directory, "--unit=containerd.service", "--no-pager", "--output=json", "--all", "--quiet", "--grep=a|b", "_MACHINE_ID=" + cfg.MachineID}
	if !reflect.DeepEqual(got, want) {
		testContext.Fatalf("unexpected command arguments: %#v", got)
	}
	buffer := journalBoundedBuffer{limit: 3}
	if written, err := buffer.Write([]byte("abcd")); err == nil || written != 3 || !buffer.overflow || buffer.String() != "abc" {
		testContext.Fatalf("journal output exceeded bounded buffer: %d %v %+v", written, err, buffer)
	}
	if _, err := decodeJournalRecords([]byte(strings.Repeat("x", journalMaxOutput+1))); err == nil {
		testContext.Fatal("oversized journal response accepted")
	}
	cfg.Timeout = time.Hour
	if journalTimeout(cfg) > 25*time.Second {
		testContext.Fatal("journal query timeout not bounded")
	}
}

func TestJournalDeployedRawMessageNeedsExactlyOneQuotedTokenDecode(testContext *testing.T) {
	raw := `{"_UID":"0","__CURSOR":"s=88615490b55c47eea6a6629edb6e75f6;i=2e767;b=6644a471edb642618328f5a5a02fc2f0;m=e647d0175;t=65e0053539903;x=e2cd7d7ec82e1298","_SYSTEMD_UNIT":"containerd.service","__MONOTONIC_TIMESTAMP":"61815456117","_TRANSPORT":"stdout","MESSAGE":"time=\"2026-10-17T10:49:11.260377427+08:00\" level=info msg=\"RunPodSandbox for name:\\\"instance-d6ae5e78-844a-4731-af16-a10b0e384ccd-0\\\"  uid:\\\"481c72aa-da10-4544-ba08-29f090cf6e4d\\\"  namespace:\\\"ani-tenant-92b09b38-ce59-4a4f-9293-f2b33148d799\\\"\"","__REALTIME_TIMESTAMP":"1792205351262467","_COMM":"containerd","_MACHINE_ID":"2e5cc7ce6b9c4666a1943a6fa65c1858","_BOOT_ID":"6644a471edb642618328f5a5a02fc2f0","_EXE":"/usr/local/bin/containerd"}
{"_TRANSPORT":"stdout","__REALTIME_TIMESTAMP":"1792205351879481","_MACHINE_ID":"2e5cc7ce6b9c4666a1943a6fa65c1858","__CURSOR":"s=88615490b55c47eea6a6629edb6e75f6;i=2e775;b=6644a471edb642618328f5a5a02fc2f0;m=e64866bac;t=65e00535d0339;x=3d913b9d4e37b882","MESSAGE":"time=\"2026-10-17T10:49:11.878947196+08:00\" level=info msg=\"RunPodSandbox for name:\\\"instance-d6ae5e78-844a-4731-af16-a10b0e384ccd-0\\\"  uid:\\\"481c72aa-da10-4544-ba08-29f090cf6e4d\\\"  namespace:\\\"ani-tenant-92b09b38-ce59-4a4f-9293-f2b33148d799\\\" returns sandbox id \\\"dd5a38fedde4838ab5afbb763954ff29fcee2c93e82b2b3d65544a6f01c14d36\\\"\"","_UID":"0","_BOOT_ID":"6644a471edb642618328f5a5a02fc2f0","_COMM":"containerd","_EXE":"/usr/local/bin/containerd","_SYSTEMD_UNIT":"containerd.service","__MONOTONIC_TIMESTAMP":"61816073132"}`
	records, err := decodeJournalRecords([]byte(raw))
	if err != nil || len(records) != 2 {
		testContext.Fatalf("deployed raw decode: records=%+v error=%v", records, err)
	}
	for _, record := range records {
		match := journalPod.FindStringSubmatch(record.Message)
		if strings.Contains(record.Message, `\`) || match == nil || match[1] != "instance-d6ae5e78-844a-4731-af16-a10b0e384ccd-0" || match[2] != "481c72aa-da10-4544-ba08-29f090cf6e4d" || match[3] != "ani-tenant-92b09b38-ce59-4a4f-9293-f2b33148d799" {
			testContext.Fatalf("actual quoted token decoding mismatch: %q", record.Message)
		}
	}
	if got := journalPod.FindStringSubmatch(records[1].Message)[5]; got != "dd5a38fedde4838ab5afbb763954ff29fcee2c93e82b2b3d65544a6f01c14d36" {
		testContext.Fatalf("sandbox identity changed: %q", got)
	}
}

func TestJournalRecoveryBudgetCoversDeployedScanWithinParentDeadline(testContext *testing.T) {
	if got := journalTimeout(JournalConfig{}); got < 20*time.Second || got >= 30*time.Second {
		testContext.Fatalf("default recovery budget %v cannot cover deployed 6.86s first scan and followup scans inside parent 30s", got)
	}
	if got := journalTimeout(JournalConfig{Timeout: time.Hour}); got != 25*time.Second {
		testContext.Fatalf("unbounded recovery timeout: %v", got)
	}
	cfg, pod, lines := journalFixture()
	expectedCursor := makeJournalFixtureCursor(journalTestBoot, 100, 1600000001000000)
	for index := range lines {
		var record map[string]string
		if err := json.Unmarshal(lines[index], &record); err != nil {
			testContext.Fatal(err)
		}
		record["__CURSOR"] = makeJournalFixtureCursor(record["_BOOT_ID"], int64((index+1)*100), 1600000001000000)
		lines[index], _ = json.Marshal(record)
	}
	calls := 0
	query := journalQuery(lines)
	statuses, ok, err := recoverJournalExitWith(context.Background(), cfg, pod, func(ctx context.Context, config JournalConfig, grep string) ([]journalRecord, error) {
		args := journalArguments(config, grep)
		cursor := ""
		for _, arg := range args {
			if strings.HasPrefix(arg, "--cursor=") {
				cursor = strings.TrimPrefix(arg, "--cursor=")
			}
		}
		if calls == 0 && cursor != "" {
			testContext.Fatal("initial UID query skipped original history")
		}
		if calls > 0 && cursor != expectedCursor {
			testContext.Fatalf("followup query did not seek exact earliest trusted cursor: %#v", args)
		}
		if deadline, exists := ctx.Deadline(); !exists || time.Until(deadline) < 19*time.Second || time.Until(deadline) > 20*time.Second {
			testContext.Fatal("query budget not preserved")
		}
		calls++
		return query(ctx, config, grep)
	})
	if err != nil || !ok || len(statuses) != 1 || calls != 3 {
		testContext.Fatalf("bounded cursor recovery failed: %+v ok=%v error=%v calls=%d", statuses, ok, err, calls)
	}
}

func makeJournalFixtureCursor(boot string, monotonic, realtime int64) string {
	return "s=88615490b55c47eea6a6629edb6e75f6;i=1;b=" + boot + ";m=" + strconv.FormatInt(monotonic, 16) + ";t=" + strconv.FormatInt(realtime, 16) + ";x=1"
}

func TestJournalRecoveryCursorUsesSameBootMonotonicOrder(testContext *testing.T) {
	first := journalRecord{Boot: journalTestBoot, Monotonic: 100, Realtime: 1600000001000000}
	first.Cursor = makeJournalFixtureCursor(first.Boot, first.Monotonic, first.Realtime)
	second := journalRecord{Boot: journalTestBoot, Monotonic: 200, Realtime: 1600000000000000}
	second.Cursor = makeJournalFixtureCursor(second.Boot, second.Monotonic, second.Realtime)
	if got := journalRecoveryCursor([]journalRecord{second, first}); got != first.Cursor {
		testContext.Fatalf("wall clock rollback changed earliest source location: %q", got)
	}
	second.Boot = strings.Repeat("a", 32)
	if got := journalRecoveryCursor([]journalRecord{first, second}); got != "" {
		testContext.Fatalf("cross-boot history was truncated: %q", got)
	}
	for _, cursor := range []string{"untrusted-cursor", makeJournalFixtureCursor(strings.Repeat("b", 32), 100, first.Realtime), makeJournalFixtureCursor(first.Boot, 101, first.Realtime), makeJournalFixtureCursor(first.Boot, 100, first.Realtime+1)} {
		first.Cursor = cursor
		if got := journalRecoveryCursor([]journalRecord{first}); got != "" {
			testContext.Fatalf("cursor conflicted with trusted source metadata: %q", got)
		}
	}
	if journalTimeout(JournalConfig{TimeoutSeconds: 60}) != 25*time.Second || journalTimeout(JournalConfig{TimeoutSeconds: 12}) != 12*time.Second {
		testContext.Fatal("operator timeout configuration is not bounded")
	}
	parent, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	cfg, pod, _ := journalFixture()
	_, _, err := recoverJournalExitWith(parent, cfg, pod, func(ctx context.Context, _ JournalConfig, _ string) ([]journalRecord, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		testContext.Fatalf("recovery exceeded parent deadline: %v", err)
	}
}
