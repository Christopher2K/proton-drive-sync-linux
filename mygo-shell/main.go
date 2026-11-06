package main

import (
	_ "embed"
	"fmt"
	"log"
	"os"

	"github.com/egoist/mygo"
	"github.com/egoist/mygo/ui"
)

//go:embed resources/icon.png
var iconBytes []byte

const (
	appTitle   = "Proton Drive Sync"
	appTagline = "Unofficial Linux client"
	disclosure = "This is a third-party application not officially supported by Proton."
)

func main() {
	a := newApp()

	mygo.App.SetName(appTitle)

	if !mygo.App.RequestSingleInstanceLock() {
		return
	}
	mygo.App.OnSecondInstance(func(args []string, workingDir string) {
		a.showWindow()
	})

	minimized := false
	for _, arg := range os.Args[1:] {
		if arg == "--minimized" {
			minimized = true
		}
	}
	if mygo.App.WasOpenedAtLogin() {
		minimized = true
	}

	mygo.App.OnWillQuit(func(e *mygo.QuitEvent) {
		a.stopEngine()
	})

	mygo.App.WhenReady(func() {
		a.autostartEnabled = mygo.App.OpenAtLogin()

		win := mygo.NewWindow(mygo.WindowOptions{
			Title:     appTitle,
			Width:     1080,
			Height:    720,
			MinWidth:  900,
			MinHeight: 620,
			Hidden:    minimized,
			StateKey:  "main",
			Content:   ui.View(a.view),
		})
		a.win = win

		win.OnClose(func(e *mygo.CloseEvent) {
			e.PreventDefault()
			a.hideWindow()
		})

		tray, err := mygo.NewTray(mygo.TrayOptions{
			Icon:    iconBytes,
			ToolTip: appTitle,
			Menu: mygo.NewMenu([]*mygo.MenuItem{
				{Label: "Open Dashboard", Click: func(*mygo.MenuItem, *mygo.Window) { a.showWindow() }},
				{Label: "Pause/Resume syncing", Click: func(*mygo.MenuItem, *mygo.Window) { a.togglePause() }},
				{Label: "Open Sync Folder", Click: func(*mygo.MenuItem, *mygo.Window) { a.openSyncFolder() }},
				{Label: "Preferences…", Click: func(*mygo.MenuItem, *mygo.Window) { a.setPage(pageSettings); a.showWindow() }},
				mygo.Separator(),
				{Label: fmt.Sprintf("Quit %s", appTitle), Click: func(*mygo.MenuItem, *mygo.Window) { a.quit() }},
			}),
		})
		if err != nil {
			log.Printf("tray: %v", err)
		} else {
			a.tray = tray
		}

		go a.startEngine()
	})

	if err := mygo.App.Run(); err != nil {
		log.Fatal(err)
	}
}
