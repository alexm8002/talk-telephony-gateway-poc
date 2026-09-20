# Nextcloud Talk Telephony Gateway

SIP/PSTN gateway for Nextcloud Talk.

The gateway connects Nextcloud Talk to an Asterisk/FreePBX server and supports incoming and outgoing telephone calls.

## Requirements

* Nextcloud
* Nextcloud Talk
* Nextcloud Spreed Signaling / HPB
* Talk Telephony Nextcloud plugin
* Asterisk / FreePBX
* Debian host for the gateway

### Fresh Debian installation

On a minimal or fresh Debian installation, the package index should be updated before installing the gateway dependencies. It is also recommended to install the latest available system updates:

```bash
apt-get update
apt-get upgrade -y
```

Then install all packages required to download, build and run the gateway:

```bash
apt-get install -y \
  ca-certificates \
  curl \
  unzip \
  golang-go \
  build-essential \
  pkg-config \
  libopus-dev
```

On very minimal Debian installations, `sudo` may not be installed. The commands above can be run directly as `root`. If using a regular user with `sudo` privileges, simply prefix the commands with `sudo`.

## 1. Install the Nextcloud plugin

Install the `talk_telephony` plugin in:

```text
/var/www/html/custom_apps/talk_telephony
```

Enable it:

```bash
php occ app:enable talk_telephony
```

Open the **Talk Telephony** administration settings and generate a **Gateway API Token**.

Users and SIP extensions are then configured directly from the Talk Telephony interface.

The plugin automatically synchronizes the extension with the native Nextcloud Talk phone-number mapping.

No `accounts.json` file is required by the gateway.

## 2. Configure SIP in Nextcloud Talk

Nextcloud Talk SIP support must be enabled.

Configure:

```text
sip_bridge_shared_secret
sip_bridge_dialin_info
sip_dialout = yes
```

Keep the value of:

```text
sip_bridge_shared_secret
```

It will be used as `SPREED_SIP_SECRET` on the gateway.

The Spreed Signaling / HPB server must also have an internal client secret configured:

```ini
[clients]
internalsecret = YOUR_HPB_INTERNAL_SECRET
```

This value will be used as `HPB_INTERNAL_SECRET`.

## 3. Install the gateway

On a fresh Debian system:

```bash
apt update

apt install -y \
  ca-certificates \
  curl \
  unzip \
  golang-go \
  build-essential \
  pkg-config \
  libopus-dev
```

Extract the gateway:

```bash
mkdir -p /opt/talk-telephony-gateway
cd /opt/talk-telephony-gateway
unzip /path/to/talk-telephony-gateway.zip
```

Build it:

```bash
./build.sh
```

## 4. Configure the gateway

Create the environment file:

```bash
cp .env.example .env
chmod 600 .env
nano .env
```

Example:

```ini
# Nextcloud
NEXTCLOUD_URL=https://NEXTCLOUD_HOST/

# Spreed Signaling / HPB
HPB_URL=wss://HPB_HOST/spreed
HPB_INTERNAL_SECRET=YOUR_HPB_INTERNAL_SECRET
WS_KEEPALIVE=15s

# Talk Telephony plugin
NEXTCLOUD_GATEWAY_TOKEN=YOUR_GATEWAY_TOKEN
NEXTCLOUD_ACCOUNTS_REFRESH=30s

# Asterisk / FreePBX
SIP_SERVER=ASTERISK_IP:5060
SIP_DOMAIN=ASTERISK_IP
SIP_CONTACT_IP=GATEWAY_IP
SIP_INBOUND_LISTEN=0.0.0.0:5060
SIP_REGISTER_EXPIRES=300
SIP_TIMEOUT=45s
SIP_INBOUND_TIMEOUT=60s

# Nextcloud Talk SIP bridge
SPREED_SIP_SECRET=YOUR_SIP_BRIDGE_SECRET

# RTP
RTP_LOCAL_ADDR=0.0.0.0:0
RTP_ADVERTISE_IP=GATEWAY_IP
```

The gateway retrieves all SIP accounts automatically from Nextcloud.

## 5. Configure Asterisk / FreePBX

Create the SIP extensions in Asterisk/FreePBX.

The credentials must match those configured for each user in the Talk Telephony Nextcloud plugin.

If the same extension is used by both a physical SIP phone and Nextcloud Talk, configure the AOR to accept multiple contacts.

For example:

```text
Max Contacts >= 2
```

Recommended PJSIP options:

```text
direct_media = no
rtp_symmetric = yes
force_rport = yes
rewrite_contact = yes
```

## 6. Test the gateway

Load the environment:

```bash
set -a
. ./.env
set +a
```

Start:

```bash
./telephony-gateway-poc
```

The gateway should:

```text
Connect to Nextcloud
Load SIP accounts
Connect to the HPB
REGISTER each enabled extension with Asterisk
Listen for incoming SIP calls
```

## 7. Install as a systemd service

Create a dedicated user:

```bash
useradd --system \
  --no-create-home \
  --shell /usr/sbin/nologin \
  telephony-gateway
```

Set permissions:

```bash
chown -R telephony-gateway:telephony-gateway \
  /opt/talk-telephony-gateway
```

Install the provided service:

```bash
cp systemd-example.service \
  /etc/systemd/system/talk-telephony-gtw.service
```

Check that its paths match the installation directory:

```ini
[Service]
WorkingDirectory=/opt/talk-telephony-gateway
EnvironmentFile=/opt/talk-telephony-gateway/.env
ExecStart=/opt/talk-telephony-gateway/telephony-gateway-poc

User=telephony-gateway
Group=telephony-gateway
```

Then:

```bash
systemctl daemon-reload
systemctl enable --now talk-telephony-gtw
```

Check the service:

```bash
systemctl status talk-telephony-gtw
```

Follow logs:

```bash
journalctl -u talk-telephony-gtw -f
```

## 8. Add users

Once the gateway is installed, no configuration is required on the gateway for new users.

Create the SIP extension in Asterisk/FreePBX, then configure the user in **Talk Telephony**:

```text
Extension
SIP username
SIP authentication user
SIP password
Caller ID
```

The plugin automatically:

```text
links the extension to the Nextcloud Talk user
                ↓
exposes the account to the Gateway API
                ↓
gateway detects the new account
                ↓
REGISTER with Asterisk
```

No gateway restart is required.

## Useful commands

```bash
# Gateway status
systemctl status talk-telephony-gtw

# Live logs
journalctl -u talk-telephony-gtw -f

# Restart gateway
systemctl restart talk-telephony-gtw

# Check SIP port
ss -lunp | grep 5060
```
