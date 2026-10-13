#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
version=${1:-0.1.0}
case "$version" in *[!0-9A-Za-z.+-]*|'') echo 'invalid version' >&2; exit 1;; esac
[ "$(go env GOOS)" = linux ] || { echo 'Build on Linux with libsystemd-dev installed' >&2; exit 1; }
arch=$(go env GOARCH)
revision=$(git rev-parse HEAD)
out="$(pwd)/dist"
mkdir -p "$out"
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT HUP INT TERM
name=ezgh-obs-agent-linux
CGO_ENABLED=1 go build -trimpath -ldflags "-s -w -X main.version=$version -X main.commit=$revision" -o "$work/$name" ./cmd/ezgh-obs-agent-linux
python3 scripts/sbom.py "$version" > "$out/${name}_${version}_${arch}.cdx.json"
cp LICENSE README.md examples/config.yaml packaging/$name.service "$work/"
tar -C "$work" -czf "$out/${name}_${version}_linux_${arch}.tar.gz" "$name" LICENSE README.md config.yaml "$name.service"
root="$work/deb"
mkdir -p "$root/usr/bin" "$root/lib/systemd/system" "$root/etc/$name" "$root/DEBIAN"
cp "$work/$name" "$root/usr/bin/"
cp packaging/$name.service "$root/lib/systemd/system/"
cp examples/config.yaml "$root/etc/$name/config.yaml"
cp packaging/postinst packaging/prerm packaging/postrm "$root/DEBIAN/"
chmod 755 "$root/DEBIAN/postinst" "$root/DEBIAN/prerm" "$root/DEBIAN/postrm"
printf '/etc/%s/config.yaml\n' "$name" > "$root/DEBIAN/conffiles"
cat > "$root/DEBIAN/control" <<CONTROL
Package: $name
Version: $version
Architecture: $arch
Maintainer: EZGH Cloud <support@ezghcloud.com>
Depends: libc6 (>= 2.31), libsystemd0, ca-certificates, passwd
Section: admin
Priority: optional
Description: EZGH Cloud Linux host metrics and log daemon
CONTROL
dpkg-deb --root-owner-group --build "$root" "$out/${name}_${version}_${arch}.deb"
case "$arch" in amd64) rpmarch=x86_64;; arm64) rpmarch=aarch64;; *) exit 1;; esac
rpmdir="$work/rpm"
mkdir -p "$rpmdir/BUILD" "$rpmdir/RPMS" "$rpmdir/SOURCES" "$rpmdir/SPECS" "$rpmdir/SRPMS"
cat > "$rpmdir/SPECS/agent.spec" <<SPEC
Name: $name
Version: $version
Release: 1
Summary: EZGH Cloud Linux host metrics and log daemon
License: Apache-2.0
BuildArch: $rpmarch
Requires: systemd-libs, ca-certificates, shadow-utils
AutoReqProv: no
%description
Collects host metrics and logs and exports OTLP to EZGH Cloud.
%install
mkdir -p %{buildroot}/usr/bin %{buildroot}/usr/lib/systemd/system %{buildroot}/etc/$name
cp $work/$name %{buildroot}/usr/bin/
cp $(pwd)/packaging/$name.service %{buildroot}/usr/lib/systemd/system/
cp $(pwd)/examples/config.yaml %{buildroot}/etc/$name/config.yaml
%post
$(cat packaging/postinst | tail -n +3)
%preun
if [ \$1 -eq 0 ] && [ -d /run/systemd/system ]; then systemctl disable --now $name.service || true; fi
%postun
if [ -d /run/systemd/system ]; then systemctl daemon-reload; fi
%files
/usr/bin/$name
/usr/lib/systemd/system/$name.service
%config(noreplace) /etc/$name/config.yaml
SPEC
rpmbuild --define "_topdir $rpmdir" --define '_build_id_links none' --define '__os_install_post %{nil}' --target "$rpmarch" -bb "$rpmdir/SPECS/agent.spec"
find "$rpmdir/RPMS" -name '*.rpm' -exec cp {} "$out/" \;
(cd "$out" && sha256sum ./*.deb ./*.rpm ./*.tar.gz ./*.cdx.json > SHA256SUMS)
