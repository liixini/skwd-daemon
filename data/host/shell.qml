import Quickshell
import Quickshell.Io
import QtQuick

ShellRoot {
  id: host

  readonly property string fifoPath: (Quickshell.env("XDG_RUNTIME_DIR") || "/tmp") + "/skwd/host-cmd"
  readonly property string launcherShellPath:     Quickshell.env("SKWD_LAUNCH_SHELL")
  readonly property string barShellPath:          Quickshell.env("SKWD_BAR_SHELL")
  readonly property string switchShellPath:       Quickshell.env("SKWD_SWITCH_SHELL")
  readonly property string notificationShellPath: Quickshell.env("SKWD_NOTIFICATION_SHELL")
  readonly property string settingsShellPath:     Quickshell.env("SKWD_SETTINGS_SHELL")
  readonly property string powerShellPath:        Quickshell.env("SKWD_POWER_SHELL")

  LazyLoader {
    id: launcherLoader
    source: host.launcherShellPath.length > 0 ? "file://" + host.launcherShellPath : ""
    activeAsync: host.launcherShellPath.length > 0
    onItemChanged: if (item) { item.showing = launcherLoader._pendingShow; launcherLoader._pendingShow = false }
    property bool _pendingShow: false
  }

  LazyLoader {
    id: barLoader
    source: host.barShellPath.length > 0 ? "file://" + host.barShellPath : ""
    activeAsync: host.barShellPath.length > 0
  }

  LazyLoader {
    id: switchLoader
    source: host.switchShellPath.length > 0 ? "file://" + host.switchShellPath : ""
    activeAsync: host.switchShellPath.length > 0
  }

  LazyLoader {
    id: notificationLoader
    source: host.notificationShellPath.length > 0 ? "file://" + host.notificationShellPath : ""
    activeAsync: host.notificationShellPath.length > 0
  }

  LazyLoader {
    id: settingsLoader
    source: host.settingsShellPath.length > 0 ? "file://" + host.settingsShellPath : ""
    activeAsync: settingsLoader._wantActive
    onItemChanged: if (item) { item.showing = settingsLoader._pendingShow; settingsLoader._pendingShow = false }
    property bool _wantActive: false
    property bool _pendingShow: false
  }

  LazyLoader {
    id: powerLoader
    source: host.powerShellPath.length > 0 ? "file://" + host.powerShellPath : ""
    activeAsync: powerLoader._wantActive
    onItemChanged: if (item) { item.showing = powerLoader._pendingShow; powerLoader._pendingShow = false }
    property bool _wantActive: false
    property bool _pendingShow: false
  }

  function _launcherToggle() {
    if (launcherLoader.item) launcherLoader.item.showing = !launcherLoader.item.showing
    else launcherLoader._pendingShow = !launcherLoader._pendingShow
  }

  function _launcherShow() {
    if (launcherLoader.item) launcherLoader.item.showing = true
    else launcherLoader._pendingShow = true
  }

  function _launcherHide() {
    if (launcherLoader.item) launcherLoader.item.showing = false
    else launcherLoader._pendingShow = false
  }

  function _barShow()   { if (barLoader.item) barLoader.item.showBar()   }
  function _barHide()   { if (barLoader.item) barLoader.item.hideBar()   }
  function _barToggle() { if (barLoader.item) barLoader.item.toggleBar() }

  function _switchOpen()    { if (switchLoader.item) switchLoader.item.open()           }
  function _switchNext()    { if (switchLoader.item) switchLoader.item.next()           }
  function _switchPrev()    { if (switchLoader.item) switchLoader.item.prev()           }
  function _switchConfirm() { if (switchLoader.item) switchLoader.item.confirm()        }
  function _switchCancel()  { if (switchLoader.item) switchLoader.item.cancel()         }
  function _switchClose()   { if (switchLoader.item) switchLoader.item.closeSelected() }

  function _settingsToggle() {
    if (host.settingsShellPath.length === 0) return
    if (!settingsLoader._wantActive) {
      settingsLoader._pendingShow = true
      settingsLoader._wantActive = true
      return
    }
    if (settingsLoader.item) settingsLoader.item.showing = !settingsLoader.item.showing
    else settingsLoader._pendingShow = !settingsLoader._pendingShow
  }

  function _settingsShow() {
    if (host.settingsShellPath.length === 0) return
    if (!settingsLoader._wantActive) {
      settingsLoader._pendingShow = true
      settingsLoader._wantActive = true
      return
    }
    if (settingsLoader.item) settingsLoader.item.showing = true
    else settingsLoader._pendingShow = true
  }

  function _settingsHide() {
    if (settingsLoader.item) settingsLoader.item.showing = false
    else settingsLoader._pendingShow = false
  }

  function _powerToggle() {
    if (host.powerShellPath.length === 0) return
    if (!powerLoader._wantActive) {
      powerLoader._pendingShow = true
      powerLoader._wantActive = true
      return
    }
    if (powerLoader.item) powerLoader.item.showing = !powerLoader.item.showing
    else powerLoader._pendingShow = !powerLoader._pendingShow
  }

  function _powerShow() {
    if (host.powerShellPath.length === 0) return
    if (!powerLoader._wantActive) {
      powerLoader._pendingShow = true
      powerLoader._wantActive = true
      return
    }
    if (powerLoader.item) powerLoader.item.showing = true
    else powerLoader._pendingShow = true
  }

  function _powerHide() {
    if (powerLoader.item) powerLoader.item.showing = false
    else powerLoader._pendingShow = false
  }

  function _dispatch(cmd) {
    if (!cmd) return
    switch (cmd) {
    case "launcher.toggle": _launcherToggle(); break
    case "launcher.show":   _launcherShow();   break
    case "launcher.hide":   _launcherHide();   break
    case "bar.show":        _barShow();        break
    case "bar.hide":        _barHide();        break
    case "bar.toggle":      _barToggle();      break
    case "switch.open":     _switchOpen();     break
    case "switch.next":     _switchNext();     break
    case "switch.prev":     _switchPrev();     break
    case "switch.confirm":  _switchConfirm();  break
    case "switch.cancel":   _switchCancel();   break
    case "switch.close":    _switchClose();    break
    case "settings.toggle": _settingsToggle(); break
    case "settings.show":   _settingsShow();   break
    case "settings.hide":   _settingsHide();   break
    case "power.toggle":    _powerToggle();    break
    case "power.show":      _powerShow();      break
    case "power.hide":      _powerHide();      break
    }
  }

  Process {
    id: _fifoSetup
    command: ["sh", "-c",
      "FIFO=" + JSON.stringify(host.fifoPath) + "; " +
      "mkdir -p \"$(dirname \"$FIFO\")\"; " +
      "fuser -k \"$FIFO\" 2>/dev/null; sleep 0.1; " +
      "rm -f \"$FIFO\"; mkfifo -m 600 \"$FIFO\""]
    running: true
    onExited: _fifoReader.running = true
  }

  Process {
    id: _fifoReader
    running: false
    command: ["sh", "-c", "trap 'exit 0' TERM HUP INT; while true; do cat " + JSON.stringify(host.fifoPath) + "; done"]
    stdout: SplitParser {
      onRead: line => host._dispatch(line.trim())
    }
    onExited: _restartTimer.start()
  }

  Timer {
    id: _restartTimer
    interval: 1000
    onTriggered: _fifoReader.running = true
  }
}
