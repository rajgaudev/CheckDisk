# CheckDisk - Linux Disk Health Monitor

![Python](https://img.shields.io/badge/python-3.8+-blue)
![Platform](https://img.shields.io/badge/platform-linux-lightgrey)
![License](https://img.shields.io/badge/license-MIT-green)

A powerful, production-ready CLI tool to monitor disk health, SMART data, temperature, and I/O performance in real time.

---

##  Features

* Real-time interactive dashboard
* SMART health & error monitoring
* Temperature tracking with history
* I/O & IOPS benchmarking (fio-based)
* Disk cleaning utilities (safe + secure wipe)
* Multi-disk support (SATA, NVMe, USB, SAS)
* Cross-distro compatibility (apt, dnf, pacman, etc.)

---

## Preview

```
checkdisk.rajgaud.com
```

---

## Installation:

### 1. Install system dependencies

```bash
git clone https://github.com/rajgaudev/CheckDisk.git
cd CheckDisk
```

```bash
sudo apt update
sudo apt install -y smartmontools nvme-cli python3 python3-venv
```

### 2. Install CheckDisk

```bash
sudo cp checkdisk.py /usr/local/bin/checkdisk
sudo chmod +x /usr/local/bin/checkdisk
```

### 3. Verify installation

```bash
sudo checkdisk --help
```

---

## Usage:

### Monitor all disks (dashboard)

```bash
sudo checkdisk all
```

### Inspect a specific disk

```bash
sudo checkdisk /dev/sda
```

### Clean disk (fast wipe)

```bash
sudo checkdisk clean /dev/sda
```

### Secure erase (full disk wipe)

```bash
sudo checkdisk clean-all /dev/sda
```

---

## What You Get

The dashboard provides:

* Disk health score (%)
* Performance metrics
* Temperature & trends
* Disk usage & free space
* Interface type (NVMe / SATA / USB)
* Live I/O activity

---

## Requirements

* Linux OS
* Python 3.8+
* `smartmontools`
* `nvme-cli` (recommended for NVMe drives)
* `fio` (optional, for benchmarking)

---

## License

This project is licensed under the MIT License.

---

## Disclaimer

Disk operations (especially cleaning/erasing) are destructive.

> Always double-check the target device before running commands.

The author is not responsible for any data loss.

---

## Support

If you find this project useful:

* Star the repository
* Report issues
* Suggest features

---

Built for developers, sysadmins, and power users who want full control over disk health.
