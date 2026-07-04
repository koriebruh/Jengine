"use client";

import { useState } from "react";
import { Menu } from "lucide-react";
import { Button } from "@/components/ui/button";
import {
  Sheet,
  SheetContent,
  SheetHeader,
  SheetTitle,
  SheetTrigger,
} from "@/components/ui/sheet";
import { SidebarNav } from "./sidebar";

/**
 * Topbar: tenant name + user menu placeholders (no real auth wiring yet -
 * frontend task 02, plans/task/frontend/01 Implementation Notes). Below
 * the md breakpoint, its menu button opens the nav as a Sheet/drawer
 * instead of the desktop-only <Sidebar> (components/layout/sidebar.tsx)
 * being visible.
 */
export function Topbar() {
  const [mobileNavOpen, setMobileNavOpen] = useState(false);

  return (
    <header className="flex h-14 items-center gap-3 border-b bg-background px-4">
      <Sheet open={mobileNavOpen} onOpenChange={setMobileNavOpen}>
        <SheetTrigger
          render={
            <Button variant="ghost" size="icon" className="md:hidden" />
          }
        >
          <Menu className="size-5" />
          <span className="sr-only">Open navigation</span>
        </SheetTrigger>
        <SheetContent side="left" className="w-64 p-0">
          <SheetHeader className="h-14 justify-center border-b px-4">
            <SheetTitle>Jengine</SheetTitle>
          </SheetHeader>
          <SidebarNav onNavigate={() => setMobileNavOpen(false)} />
        </SheetContent>
      </Sheet>

      <span className="text-sm font-medium md:hidden">Jengine</span>

      <div className="ml-auto flex items-center gap-3">
        {/* Tenant name placeholder - real tenant resolution is frontend
         * task 02's job. */}
        <span className="rounded-md border px-2 py-1 text-xs text-muted-foreground">
          Acme Bank
        </span>
        {/* User menu placeholder - no real auth wiring yet. */}
        <Button variant="ghost" size="icon" className="rounded-full">
          <span className="text-xs font-semibold">U</span>
        </Button>
      </div>
    </header>
  );
}
