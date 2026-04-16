%global crate skwd-daemon

Name:           skwd-daemon
Version:        0.1.0
Release:        1%{?dist}
Summary:        Daemon for Skwd Shell, a collection of Quickshell programs and widgets

License:        MIT
URL:            https://github.com/liixini/skwd-daemon
Source0:        %{url}/archive/refs/heads/main.tar.gz#/%{name}-main.tar.gz

ExclusiveArch:  x86_64 aarch64

BuildRequires:  cargo >= 1.85
BuildRequires:  rust >= 1.85
BuildRequires:  gcc
BuildRequires:  pkgconfig

Requires:       ImageMagick

Recommends:     ffmpeg-free
Recommends:     ollama

Suggests:       steamcmd
Suggests:       linux-wallpaperengine

%description
Daemon and CLI for Skwd-wall, a Quickshell-based wallpaper selector with
color sorting, Matugen integration, tag system, and Wallhaven/Steam browsing.

The daemon handles background tasks like wallpaper processing, database
management, and caching. The CLI provides command-line control.

%prep
%autosetup -n %{name}-main

%build
export RUSTUP_TOOLCHAIN=stable
export CARGO_TARGET_DIR=target
cargo build --release

%install
install -Dpm 0755 target/release/skwd-daemon %{buildroot}%{_bindir}/skwd-daemon
install -Dpm 0755 target/release/skwd %{buildroot}%{_bindir}/skwd
install -Dpm 0644 data/skwd-daemon.service %{buildroot}%{_userunitdir}/skwd-daemon.service
install -Dpm 0644 LICENSE %{buildroot}%{_datadir}/licenses/%{name}/LICENSE

# Systemd user preset - auto-enable on first login
mkdir -p %{buildroot}%{_userpresetdir}
echo "enable skwd-daemon.service" > %{buildroot}%{_userpresetdir}/90-skwd-daemon.preset

%post
systemctl --global preset skwd-daemon.service 2>/dev/null || :

%preun
if [ $1 -eq 0 ]; then
  systemctl --global disable skwd-daemon.service 2>/dev/null || :
fi

%files
%license LICENSE
%{_bindir}/skwd-daemon
%{_bindir}/skwd
%{_userunitdir}/skwd-daemon.service
%{_userpresetdir}/90-skwd-daemon.preset
