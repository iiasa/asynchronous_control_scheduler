# Building Static OpenSSH Using musl on Ubuntu 22.04

This guide walks you through building a **statically linked OpenSSH client** using **musl libc** on Ubuntu 22.04. It includes building required dependencies (zlib and OpenSSL) statically with `musl-gcc`.

---

## 🛠 Prerequisites

Install essential build tools and musl:

```bash
sudo apt update
sudo apt install build-essential musl-tools curl wget
```

---

## 📦 Build and Install zlib (Static)

```bash
wget https://zlib.net/zlib-1.3.tar.gz
tar xzf zlib-1.3.tar.gz
cd zlib-1.3

CC=musl-gcc ./configure --static --prefix=/opt/zlib-musl
make
make install
```

---

## 🔐 Build and Install OpenSSL (Static)

```bash
wget https://www.openssl.org/source/openssl-3.3.0.tar.gz
tar xzf openssl-3.3.0.tar.gz
cd openssl-3.3.0

./Configure linux-x86_64 \
  no-shared \
  no-dso \
  no-tests \
  no-ssl3 \
  no-comp \
  no-secure-memory \
  no-afalgeng \
  --prefix=/opt/openssl-musl \
  --openssldir=/opt/openssl-musl

make CC=musl-gcc
make install_sw
```

---

## 🔐 Build OpenSSH (Static)

```bash
wget https://cdn.openbsd.org/pub/OpenBSD/OpenSSH/portable/openssh-9.7p1.tar.gz
tar xzf openssh-9.7p1.tar.gz
cd openssh-9.7p1

./configure \
  --host=x86_64-linux-musl \
  --disable-shared \
  --without-pam \
  --without-openssl-version-check \
  --prefix=/opt/openssh-musl \
  CC=musl-gcc \
  CFLAGS="-I/opt/openssl-musl/include -I/opt/zlib-musl/include" \
  LDFLAGS="-L/opt/openssl-musl/lib64 -L/opt/zlib-musl/lib -static"

make
make install
```

---

## ✅ Verify Static SSH Binary

```bash
file /opt/openssh-musl/bin/ssh
ldd /opt/openssh-musl/bin/ssh
```

You should see:
- "statically linked"
- "not a dynamic executable"

---

## 🏁 Result

Your static SSH binary will be available at:

```bash
/opt/openssh-musl/bin/ssh
```

You can now copy it to other systems without worrying about dynamic dependencies.

---

## Notes

- You can change installation prefixes as needed.
- Make sure your `musl-gcc` is working and points to the correct musl toolchain.
