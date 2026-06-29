import {
  AppShell,
  Badge,
  Button,
  Card,
  Center,
  Group,
  NavLink,
  Paper,
  PasswordInput,
  SimpleGrid,
  Stack,
  Table,
  Text,
  TextInput,
  Textarea,
  Title,
} from "@mantine/core";
import { useInterval } from "@mantine/hooks";
import { notifications } from "@mantine/notifications";
import { useEffect, useState } from "react";
import { api, type Info } from "./api";

const SECTIONS = ["Overview", "Channels", "Publish"] as const;
type Section = (typeof SECTIONS)[number];

export function App() {
  const [authed, setAuthed] = useState<boolean | null>(null);
  useEffect(() => {
    api.me().then((r) => setAuthed(r.authenticated)).catch(() => setAuthed(false));
  }, []);

  if (authed === null) return null;
  if (!authed) return <Login onLogin={() => setAuthed(true)} />;
  return <Dashboard />;
}

function Login({ onLogin }: { onLogin: () => void }) {
  const [password, setPassword] = useState("");
  const [busy, setBusy] = useState(false);
  const submit = async () => {
    setBusy(true);
    try {
      await api.login(password);
      onLogin();
    } catch {
      notifications.show({ color: "red", message: "Wrong password" });
    } finally {
      setBusy(false);
    }
  };
  return (
    <Center h="100vh">
      <Paper withBorder p="xl" w={360}>
        <Stack>
          <Title order={3}>Manifold — Admin</Title>
          <PasswordInput
            label="Password"
            value={password}
            onChange={(e) => setPassword(e.currentTarget.value)}
            onKeyDown={(e) => e.key === "Enter" && submit()}
          />
          <Button loading={busy} onClick={submit}>
            Sign in
          </Button>
        </Stack>
      </Paper>
    </Center>
  );
}

function Dashboard() {
  const [active, setActive] = useState<Section>("Overview");
  return (
    <AppShell header={{ height: 56 }} navbar={{ width: 220, breakpoint: "sm" }} padding="md">
      <AppShell.Header>
        <Group h="100%" px="md" justify="space-between">
          <Title order={4}>Manifold — Admin</Title>
          <Badge color="green" variant="dot">
            live
          </Badge>
        </Group>
      </AppShell.Header>
      <AppShell.Navbar p="xs">
        {SECTIONS.map((s) => (
          <NavLink key={s} label={s} active={active === s} onClick={() => setActive(s)} />
        ))}
      </AppShell.Navbar>
      <AppShell.Main>
        {active === "Overview" && <Overview />}
        {active === "Channels" && <Channels />}
        {active === "Publish" && <Publish />}
      </AppShell.Main>
    </AppShell>
  );
}

function Stat({ label, value }: { label: string; value: number | string }) {
  return (
    <Card withBorder>
      <Text size="xs" c="dimmed" tt="uppercase">
        {label}
      </Text>
      <Text fw={700} size="xl">
        {value}
      </Text>
    </Card>
  );
}

function Overview() {
  const [info, setInfo] = useState<Info | null>(null);
  const poll = useInterval(() => api.info().then(setInfo).catch(() => {}), 2000);
  useEffect(() => {
    api.info().then(setInfo).catch(() => {});
    poll.start();
    return poll.stop;
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  if (!info) return <Text>Loading…</Text>;
  return (
    <Stack>
      <Title order={3}>Overview · {info.node}</Title>
      <SimpleGrid cols={{ base: 2, md: 4 }}>
        <Stat label="Connections" value={info.num_connections} />
        <Stat label="Channels" value={info.num_channels} />
        <Stat label="Published" value={info.messages_published} />
        <Stat label="Subscriptions" value={info.subscriptions} />
        <Stat label="Opened total" value={info.connections_opened} />
        <Stat label="Closed total" value={info.connections_closed} />
      </SimpleGrid>
    </Stack>
  );
}

function Channels() {
  const [channels, setChannels] = useState<string[]>([]);
  const [users, setUsers] = useState<Record<string, string[]>>({});
  const load = () => api.channels().then((r) => setChannels(r.channels)).catch(() => {});
  useEffect(() => {
    load();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);
  return (
    <Stack>
      <Group>
        <Title order={3}>Channels</Title>
        <Button size="xs" variant="light" onClick={load}>
          Refresh
        </Button>
      </Group>
      {channels.length === 0 ? (
        <Text c="dimmed">No active channels</Text>
      ) : (
        <Table>
          <Table.Thead>
            <Table.Tr>
              <Table.Th>Channel</Table.Th>
              <Table.Th>Presence</Table.Th>
            </Table.Tr>
          </Table.Thead>
          <Table.Tbody>
            {channels.map((c) => (
              <Table.Tr key={c} onClick={() => api.presence(c).then((r) => setUsers((u) => ({ ...u, [c]: r.users })))}>
                <Table.Td style={{ cursor: "pointer" }}>{c}</Table.Td>
                <Table.Td>{users[c] ? users[c].join(", ") || "—" : "click"}</Table.Td>
              </Table.Tr>
            ))}
          </Table.Tbody>
        </Table>
      )}
    </Stack>
  );
}

function Publish() {
  const [channel, setChannel] = useState("");
  const [data, setData] = useState("");
  const [busy, setBusy] = useState(false);
  const submit = async () => {
    setBusy(true);
    try {
      const r = await api.publish(channel, data);
      notifications.show({ color: "green", message: `Published, offset ${r.offset}` });
    } catch (e: any) {
      notifications.show({ color: "red", message: String(e.message ?? e) });
    } finally {
      setBusy(false);
    }
  };
  return (
    <Stack maw={520}>
      <Title order={3}>Publish</Title>
      <TextInput label="Channel" placeholder="news:sports" value={channel} onChange={(e) => setChannel(e.currentTarget.value)} />
      <Textarea label="Message" minRows={3} value={data} onChange={(e) => setData(e.currentTarget.value)} />
      <Button loading={busy} onClick={submit} disabled={!channel}>
        Publish
      </Button>
    </Stack>
  );
}
