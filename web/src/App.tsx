import { AppShell, Group, NavLink, Title, Badge } from "@mantine/core";
import { useState } from "react";

// Разделы админки (см. docs/architecture.md, раздел 9).
const SECTIONS = [
  "Overview",
  "Channels",
  "Connections",
  "Publish",
  "Namespaces",
  "Metrics",
] as const;
type Section = (typeof SECTIONS)[number];

export function App() {
  const [active, setActive] = useState<Section>("Overview");

  return (
    <AppShell
      header={{ height: 56 }}
      navbar={{ width: 220, breakpoint: "sm" }}
      padding="md"
    >
      <AppShell.Header>
        <Group h="100%" px="md" justify="space-between">
          <Title order={4}>Socket — Admin</Title>
          <Badge color="green" variant="dot">
            node: socket-1
          </Badge>
        </Group>
      </AppShell.Header>

      <AppShell.Navbar p="xs">
        {SECTIONS.map((s) => (
          <NavLink
            key={s}
            label={s}
            active={active === s}
            onClick={() => setActive(s)}
          />
        ))}
      </AppShell.Navbar>

      <AppShell.Main>
        {/* TODO(impl): рендер раздела {active}: таблицы (mantine-datatable),
            графики (@mantine/charts из $metrics через @socket/client), формы публикации. */}
        <Title order={3}>{active}</Title>
      </AppShell.Main>
    </AppShell>
  );
}
