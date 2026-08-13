%global crate skwd-daemon

Name:           skwd-daemon
Version:        0.1.0
Release:        3%{?dist}
Summary:        Daemon for Skwd Shell, a collection of Quickshell programs and widgets

License:        MIT
URL:            https://github.com/liixini/skwd-daemon
Source0:        %{url}/archive/refs/heads/main.tar.gz#/%{name}-main.tar.gz

ExclusiveArch:  x86_64 aarch64

BuildRequires:  cargo >= 1.85
BuildRequires:  rust >= 1.85
BuildRequires:  gcc
BuildRequires:  pkgconfig
BuildRequires:  systemd-rpm-macros
BuildRequires:  clang-devel
BuildRequires:  pkgconfig(libavcodec)
BuildRequires:  pkgconfig(libavdevice)
BuildRequires:  pkgconfig(libavfilter)
BuildRequires:  pkgconfig(libavformat)
BuildRequires:  pkgconfig(libavutil)
BuildRequires:  pkgconfig(libswresample)
BuildRequires:  pkgconfig(libswscale)
BuildRequires:  pkgconfig(alsa)
BuildRequires:  pkgconfig(libpulse)
BuildRequires:  pkgconfig(libpulse-simple)
BuildRequires:  pkgconfig(wayland-client)
BuildRequires:  pkgconfig(wayland-protocols)
BuildRequires:  pkgconfig(wayland-egl)
BuildRequires:  pkgconfig(egl)

Requires:       ImageMagick
Requires:       /usr/bin/ffmpeg
Requires:       /usr/bin/ffprobe
Requires:       qt6-qttools

Suggests:       ollama
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
install -Dpm 0755 target/release/skwd-paper %{buildroot}%{_bindir}/skwd-paper
install -Dpm 0755 target/release/skwd-paper-still %{buildroot}%{_bindir}/skwd-paper-still
install -Dpm 0644 target/release/libsteam_api.so %{buildroot}%{_prefix}/lib/%{name}/libsteam_api.so
install -Dpm 0644 data/skwd-daemon.service %{buildroot}%{_prefix}/lib/systemd/user/skwd-daemon.service
install -Dpm 0644 LICENSE %{buildroot}%{_datadir}/licenses/%{name}/LICENSE

# Systemd user preset - auto-enable on first login
mkdir -p %{buildroot}%{_prefix}/lib/systemd/user-preset
echo "enable skwd-daemon.service" > %{buildroot}%{_prefix}/lib/systemd/user-preset/90-skwd-daemon.preset

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
%{_bindir}/skwd-paper
%{_bindir}/skwd-paper-still
%{_prefix}/lib/%{name}/libsteam_api.so
%{_prefix}/lib/systemd/user/skwd-daemon.service
%{_prefix}/lib/systemd/user-preset/90-skwd-daemon.preset
