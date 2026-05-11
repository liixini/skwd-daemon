import Quickshell
import Quickshell.Io
import QtQuick

ShellRoot {
  id: host

  readonly property string socketPath: (Quickshell.env("XDG_RUNTIME_DIR") || "/tmp") + "/skwd/daemon.sock"
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

  function _dispatch(ev) {
    if (!ev) return
    switch (ev) {
    case "skwd.launcher.toggle": _launcherToggle(); break
    case "skwd.launcher.show":   _launcherShow();   break
    case "skwd.launcher.hide":   _launcherHide();   break
    case "skwd.bar.show":        _barShow();        break
    case "skwd.bar.hide":        _barHide();        break
    case "skwd.bar.toggle":      _barToggle();      break
    case "skwd.switch.open":     _switchOpen();     break
    case "skwd.switch.next":     _switchNext();     break
    case "skwd.switch.prev":     _switchPrev();     break
    case "skwd.switch.confirm":  _switchConfirm();  break
    case "skwd.switch.cancel":   _switchCancel();   break
    case "skwd.switch.hide":     _switchCancel();   break
    case "skwd.switch.close":    _switchClose();    break
    case "skwd.settings.toggle": _settingsToggle(); break
    case "skwd.settings.show":   _settingsShow();   break
    case "skwd.settings.hide":   _settingsHide();   break
    case "skwd.power.toggle":    _powerToggle();    break
    case "skwd.power.show":      _powerShow();      break
    case "skwd.power.hide":      _powerHide();      break
    }
  }

  property var _socket: Socket {
    path: host.socketPath
    connected: false

    parser: SplitParser {
      onRead: line => {
        var trimmed = (line || "").trim()
        if (!trimmed) return
        var msg
        try { msg = JSON.parse(trimmed) } catch (e) { return }
        if (msg && msg.event) host._dispatch(msg.event)
      }
    }

    onConnectionStateChanged: {
      if (connected) {
        var sub = JSON.stringify({
          method: "subscribe",
          params: { events: ["skwd.bar.", "skwd.launcher.", "skwd.switch.", "skwd.settings.", "skwd.power."] },
          id: 1
        })
        write(sub + "\n")
        flush()
      } else {
        _reconnectTimer.restart()
      }
    }
  }

  property var _reconnectTimer: Timer {
    interval: 1000
    repeat: false
    onTriggered: { if (!host._socket.connected) host._socket.connected = true }
  }

  Component.onCompleted: { host._socket.connected = true }
}
