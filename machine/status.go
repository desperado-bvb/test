package machine

type MachineStatus struct {
	MachID   string
	IsAlive  bool
	MachInfo MachineInfo
}

type MachineInfo struct {
	HostName   string
	PublicIP   string
	FileName   uint16
	Offset	   uint16
}
