import type { LucideIcon } from "lucide-react";
import {
  LayoutDashboard,
  FolderKanban,
  GitCompareArrows,
  Cable,
  FileCode2,
  ShieldUser,
  ScrollText,
} from "lucide-react";

export interface NavItem {
  label: string;
  href: string;
  icon: LucideIcon;
  /**
   * Screens not yet built (frontend tasks 08/09/11) - marked so the shell
   * is honest about what's real today without hiding the eventual
   * information architecture (plans/task/frontend/01 Implementation
   * Notes), not a feature-flag system.
   */
  v1?: boolean;
}

// Fixed order - later tasks assume these exact routes and this exact
// placement (plans/task/frontend/01 Common Pitfalls).
export const NAV_ITEMS: NavItem[] = [
  { label: "Overview", href: "/", icon: LayoutDashboard },
  { label: "Cases", href: "/cases", icon: FolderKanban },
  { label: "Match Review", href: "/matches", icon: GitCompareArrows },
  { label: "Connectors", href: "/connectors", icon: Cable },
  { label: "Rules", href: "/rules", icon: FileCode2, v1: true },
  { label: "Admin", href: "/admin", icon: ShieldUser, v1: true },
  { label: "Audit", href: "/audit", icon: ScrollText, v1: true },
];
