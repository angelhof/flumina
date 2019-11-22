/* -*- Mode:C++; c-file-style:"gnu"; indent-tabs-mode:nil; -*- */
/*
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation;
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */

//
// This is an illustration of how one could use virtualization techniques to
// allow running applications on virtual machines talking over simulated
// networks.
//
// The actual steps required to configure the virtual machines can be rather
// involved, so we don't go into that here.  Please have a look at one of
// our HOWTOs on the nsnam wiki for more details about how to get the
// system confgured.  For an example, have a look at "HOWTO Use Linux
// Containers to set up virtual networks" which uses this code as an
// example.
//
// The configuration you are after is explained in great detail in the
// HOWTO, but looks like the following:
//
//  +----------+                           +----------+
//  | virtual  |                           | virtual  |
//  |  Linux   |                           |  Linux   |
//  |   Host   |                           |   Host   |
//  |          |                           |          |
//  |   eth0   |                           |   eth0   |
//  +----------+                           +----------+
//       |                                      |
//  +----------+                           +----------+
//  |  Linux   |                           |  Linux   |
//  |  Bridge  |                           |  Bridge  |
//  +----------+                           +----------+
//       |                                      |
//  +------------+                       +-------------+
//  | "tap-left" |                       | "tap-right" |
//  +------------+                       +-------------+
//       |           n0            n1           |
//       |       +--------+    +--------+       |
//       +-------|  tap   |    |  tap   |-------+
//               | bridge |    | bridge |
//               +--------+    +--------+
//               |  CSMA  |    |  CSMA  |
//               +--------+    +--------+
//                   |             |
//                   |             |
//                   |             |
//                   ===============
//                      CSMA LAN
//
#include <cstdint>
#include <ctime>
#include <csignal>
#include <iostream>
#include <iomanip>
#include <fstream>
#include <sstream>
#include <vector>
#include <string>
#include <map>
#include <tuple>
#include <algorithm>

#include "ns3/core-module.h"
#include "ns3/network-module.h"
#include "ns3/csma-module.h"
#include "ns3/internet-module.h"
#include "ns3/tap-bridge-module.h"

using namespace ns3;

NS_LOG_COMPONENT_DEFINE ("TapCsmaVirtualMachineExample");

class Statistics {
public:
  Statistics()
    : packetSeries(), totalData(), packetCount(0), packetSize(0), totalSize(0),
      median(0), percentile10(0), percentile90(0)
    {}

  void SaveDataItem();
  void ProcessPacket(Ptr<const Packet> p);
  void GenerateStatistics();
  void PrintStatistics(std::ostream& os) const;
  void PrintTimeSeries(std::ostream& os) const;

private:
  using DataItem = std::tuple<Time, uint32_t, uint32_t>;
  std::vector<DataItem> packetSeries;

  using SrcDstPair = std::tuple<uint32_t, uint32_t>;
  std::map<SrcDstPair, uint32_t> totalData;

  uint32_t packetCount;
  uint32_t packetSize;
  uint32_t totalSize;

  uint32_t median;
  uint32_t percentile10;
  uint32_t percentile90;
};

void Statistics::SaveDataItem() {
  packetSeries.emplace_back(Simulator::Now(), packetCount, packetSize);
  packetCount = 0;
  packetSize = 0;
  Simulator::Schedule(MilliSeconds(500), &Statistics::SaveDataItem, this);
}

void Statistics::ProcessPacket(Ptr<const Packet> p) {
  // Extract the ethernet header
  EthernetHeader eHead;
  Ptr<Packet> copy = p->Copy();
  copy->RemoveHeader(eHead);

  // If p is an IPv4 packet (0x0800), process it, otherwise ignore
  if (eHead.GetLengthType() == 0x0800) {
    packetCount++;
    packetSize += p->GetSize();
    totalSize += p->GetSize();

    Ipv4Header ipv4Head;
    copy->RemoveHeader(ipv4Head);

    uint32_t src = ipv4Head.GetSource().Get();
    uint32_t dst = ipv4Head.GetDestination().Get();
    SrcDstPair pair = std::make_tuple(src, dst);

    if (totalData.count(pair) == 0) {
      totalData.emplace(pair, 0);
    }
    totalData[pair] += p->GetSize();
  }
}

void Statistics::GenerateStatistics() {
  // Skip the warmup and cooldown periods in packetSeries. These periods are
  // characterized by having less then 10 packets per interval.
  std::size_t start = 0;
  while (start < packetSeries.size() && std::get<1>(packetSeries[start]) < 10)
    start++;

  std::size_t end = packetSeries.size();
  while (end > start && std::get<1>(packetSeries[end - 1]) < 10)
    end--;

  if (start == end) {
    // There are no intervals with 10 or more packets :'(
    return;
  }

  std::vector<uint32_t> percentile;
  for (auto it = packetSeries.cbegin() + start; it != packetSeries.cbegin() + end; it++) {
    percentile.emplace_back(std::get<2>(*it));
  }
  std::sort(percentile.begin(), percentile.end());

  percentile10 = percentile[percentile.size() / 10 + (percentile.size() % 10 == 0 ? 0 : 1)];
  median = percentile[percentile.size() / 2 + (percentile.size() % 10 == 0 ? 0 : 1)];
  percentile90 = percentile[percentile.size() * 9 / 10 + (percentile.size() % 10 == 0 ? 0 : 1)];
}

void Statistics::PrintStatistics(std::ostream& os) const {
  os << "Total IPv4 data: " << totalSize << std::endl;
  os << "Total IPv4 data per 500 ms intervals:" << std::endl
    << " 10th percentile: " << percentile10
    << " median: " << median
    << " 90th percentile: " << percentile90 << std::endl;
  os << "Breakdown per source and destination:" << std::endl;
  for (auto const& entry : totalData) {
    uint32_t src, dst;
    std::tie (src, dst) = entry.first;
    os
      << ((src & (0xff << 24)) >> 24)
      << "." << ((src & (0xff << 16)) >> 16)
      << "." << ((src & (0xff << 8)) >> 8)
      << "." << (src & 0xff)
      << " -> "
      << ((dst & (0xff << 24)) >> 24)
      << "." << ((dst & (0xff << 16)) >> 16)
      << "." << ((dst & (0xff << 8)) >> 8)
      << "." << (dst & 0xff)
      << ": "
      << entry.second
      << std::endl;
  }
}

void Statistics::PrintTimeSeries(std::ostream& os) const {
  for (auto const& item : packetSeries) {
    Time t;
    uint32_t count;
    uint32_t size;

    std::tie (t, count, size) = item;
    os << t.GetMilliSeconds() << "," << count << "," << size << std::endl;
  }
}

void handle_sigint(int sig) {
  Simulator::Stop();
}

int
main (int argc, char *argv[])
{
  double TotalTime = 120.0;
  bool Tracing = false;
  std::string MainNode("main");
  std::string FilenamePrefix("simulation");

  CommandLine cmd;
  cmd.AddValue ("TotalTime", "Total simulation time", TotalTime);
  cmd.AddValue ("Tracing", "Output a PCAP trace of all the packets sent over the network", Tracing);
  cmd.AddValue ("MainNode", "The name of the main node", MainNode);
  cmd.AddValue ("FilenamePrefix", "Prefix for the name of the output files", FilenamePrefix);
  cmd.Usage ("All the non-option arguments count as node names.");
  cmd.Parse (argc, argv);

  int NumNodes = cmd.GetNExtraNonOptions ();
  std::vector<std::string> nodeNames{MainNode};
  for (int i = 0; i < NumNodes; i++)
    {
      nodeNames.emplace_back (cmd.GetExtraNonOption (i));
    }
  NumNodes++;

  // Append a timestamp to FilenamePrefix
  std::time_t rawtime = std::time(nullptr);
  struct std::tm *timestamp = std::localtime(&rawtime);
  std::ostringstream ss(FilenamePrefix, std::ios_base::ate);
  ss << "_" << (timestamp->tm_year + 1900)
    << "-" << std::setfill('0') << std::setw(2)
      << (timestamp->tm_mon + 1)
    << "-" << std::setfill('0') << std::setw(2)
      << timestamp->tm_mday
    << "_" << std::setfill('0') << std::setw(2)
      << timestamp->tm_hour
    << ":" << std::setfill('0') << std::setw(2)
      << timestamp->tm_min
    << ":" << std::setfill('0') << std::setw(2)
      << timestamp->tm_sec;
  FilenamePrefix = ss.str();

  //
  // We are interacting with the outside, real, world.  This means we have to
  // interact in real-time and therefore means we have to use the real-time
  // simulator and take the time to calculate checksums.
  //
  GlobalValue::Bind ("SimulatorImplementationType", StringValue ("ns3::RealtimeSimulatorImpl"));
  GlobalValue::Bind ("ChecksumEnabled", BooleanValue (true));

  //
  // Create the ghost nodes.
  //
  NS_LOG_UNCOND ("Creating nodes");
  NodeContainer nodes;
  nodes.Create (NumNodes);

  //
  // Use a CsmaHelper to get a CSMA channel created, and the needed net
  // devices installed on both of the nodes.  The data rate and delay for the
  // channel can be set through the command-line parser.  For example,
  //
  // ./waf --run "tap-vm --ns3::CsmaChannel::DataRate=10000000"
  //
  CsmaHelper csma;
  NetDeviceContainer devices = csma.Install (nodes);

  //
  // Use the TapBridgeHelper to connect to the pre-configured tap devices.
  // We go with "UseBridge" mode since the CSMA devices support
  // promiscuous mode and can therefore make it appear that the bridge is
  // extended into ns-3.  The install method essentially bridges the specified
  // tap to the specified CSMA device.
  //
  NS_LOG_UNCOND ("Creating tap bridges");
  TapBridgeHelper tapBridge;
  tapBridge.SetAttribute ("Mode", StringValue ("UseBridge"));

  for (int i = 0; i < NumNodes; i++)
    {
        std::stringstream tapName;
        tapName << "tap-" << nodeNames[i];
        NS_LOG_UNCOND ("Tap bridge = " + tapName.str ());

        tapBridge.SetAttribute ("DeviceName", StringValue (tapName.str ()));
        tapBridge.Install (nodes.Get (i), devices.Get (i));
    }

  //
  // Enable pcap tracing for the main node
  //
  if (Tracing) {
    csma.EnablePcap (FilenamePrefix + ".pcap", devices.Get (0), true, true);
  }

  Statistics stats;
  auto processPacketCallback = MakeCallback(&Statistics::ProcessPacket, &stats);
  devices.Get(0)->TraceConnectWithoutContext("PromiscSniffer", processPacketCallback);
  Simulator::Schedule(MilliSeconds(500), &Statistics::SaveDataItem, &stats);

  //
  // Run the simulation for TotalTime seconds to give the user time to play around
  //
  NS_LOG_UNCOND ("Running simulation in csma mode");
  std::signal(SIGINT, handle_sigint);
  Simulator::Stop (Seconds (TotalTime));
  Simulator::Run ();
  Simulator::Destroy ();

  stats.GenerateStatistics();

  std::ofstream statsFile(FilenamePrefix + "-stats.txt");
  stats.PrintStatistics(statsFile);
  statsFile.close();

  std::ofstream seriesFile(FilenamePrefix + "-time-series.csv");
  stats.PrintTimeSeries(seriesFile);
  seriesFile.close();
}
