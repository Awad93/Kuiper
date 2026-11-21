#!/bin/sh
set -eu

cd /tmp/kuiper-build
export DEBIAN_FRONTEND=noninteractive
# Buster is archived. Keep using Debian's signed package metadata.
cat > /etc/apt/sources.list <<'EOF'
deb [check-valid-until=no] https://archive.debian.org/debian buster main
deb [check-valid-until=no] https://archive.debian.org/debian-security buster/updates main
EOF
apt-get update
apt-get install -y --no-install-recommends \
    autoconf automake bison build-essential ca-certificates cron curl flex gettext \
    libffi-dev libjson-c-dev libldap2-dev liblzma-dev libmaxminddb-dev libnuma-dev \
    libpcap-dev libpcre2-dev librrd-dev libsasl2-dev libssl-dev libtool \
    libxml2-dev libxslt1-dev p7zip-full pkg-config python3-dev python3-pip sqlite3 zlib1g-dev

python2 -m pip install --no-cache-dir --no-deps \
    pip==20.3.4 setuptools==44.1.1 wheel==0.37.1 Cython==0.29.37
python3 -m pip install --no-cache-dir --no-deps \
    pip==23.3.2 setuptools==67.8.0 wheel==0.41.3 Cython==0.29.37

# Install the direct and transitive pins without letting pip select new versions.
python2 -m pip install --no-cache-dir --no-build-isolation --no-deps \
    -r requirements_2.7.txt -r constraints-2.7.txt
python3 -m pip install --no-cache-dir --no-build-isolation --no-deps \
    -r requirements_3.txt -r constraints-3.7.txt
python2 -m pip check
python3 -m pip check

ndpi_revision=b328916966bc39debd7f9eb832e6471088ef0220
curl -fsSL --retry 3 "https://codeload.github.com/ntop/nDPI/tar.gz/$ndpi_revision" -o ndpi.tar.gz
echo 'f0c0c502a0bae6ee681cb45a0ef7427224deef5cbaf00aecdb1662d7d68a7bfa  ndpi.tar.gz' | sha256sum -c -
mkdir ndpi
tar -xzf ndpi.tar.gz --strip-components=1 -C ndpi
(
    cd ndpi
    ./autogen.sh
    ./configure
    make -j2
    make install
)
ldconfig
command -v ndpiReader
rm -rf /var/lib/apt/lists/* /tmp/kuiper-build
