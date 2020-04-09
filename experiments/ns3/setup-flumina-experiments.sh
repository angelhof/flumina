#!/bin/bash

# This script needs to be run by root. It is tuned to Ubuntu; the part
# below that disables bridge traffic going through iptables etc. may
# need to be changed for other distributions.

# Change this to a desired directory

netutils=/home/filip/net-utils

# We create a group that will allow special network administration
# privileges required to run the Flumina experiments.

groupadd flumina

# Add different users to the group

usermod -a -G flumina filip
usermod -a -G flumina konstantinos

# A little bit of fiddling with the network namespaces is
# needed. We are creating another user group to keep everything
# kosher.

groupadd --system netns

usermod -a -G netns filip
usermod -a -G netns konstantinos
usermod -a -G netns khheo

if [ ! -d /var/run/netns ]
then
  mkdir -p /var/run/netns
fi

chown root:netns /var/run/netns
chmod 775 /var/run/netns

# We make copies of the required network utilities. The copies are
# equipped with the CAP_NET_ADMIN capability. We allow only the
# sosp-experiment users to execute these utilities.

cp /bin/ip ${netutils}
cp /sbin/brctl ${netutils}
cp /usr/bin/tunctl ${netutils}

chown root:flumina ${netutils}/*
chmod 750 ${netutils}/*

setcap cap_net_admin=eip ${netutils}/ip
setcap cap_net_admin=eip ${netutils}/brctl
setcap cap_net_admin=eip ${netutils}/tunctl

# Finally, we disable various firewall filtering of the network
# traffic over bridges.

conf=/etc/sysctl.d/80-bridge.conf

touch ${conf}

echo "net.bridge.bridge-nf-call-arptables = 0" >> ${conf}
echo "net.bridge.bridge-nf-call-iptables = 0" >> ${conf}
echo "net.bridge.bridge-nf-call-ip6tables = 0" >> ${conf}
echo "net.bridge.bridge-nf-filter-pppoe-tagged = 0" >> ${conf}
echo "net.bridge.bridge-nf-filter-vlan-tagged = 0" >> ${conf}
echo "net.bridge.bridge-nf-pass-vlan-input-dev = 0" >> ${conf}

# Apply the new configuration immediately.

service procps restart

# After a reboot, make sure the module br_netfilter is loaded
# before applying the rules from above.

echo br_netfilter > /etc/modules-load.d/bridge.conf

