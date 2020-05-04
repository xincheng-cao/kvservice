package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"
import "sync/atomic"

type ViewServer struct {
	mu       sync.Mutex
	l        net.Listener
	dead     int32			// for testing
	rpccount int32			// for testing
	me       string
	view     *View			// current view				
	newv     *View			// new view
	acked	 bool			// has the current view acked by its primary
	pttl     int			// ttl of current primary
	bttl     int			// ttl of current backup 
	idleServers map[string]int	// extra servers, server address -> ttl
}

func createView(viewno uint, primary string, backup string) (view *View) {
	view = new(View)
	view.Viewnum = viewno
	view.Primary = primary
	view.Backup  = backup
	return
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	server, viewno := args.Me, args.Viewnum

	if viewno == 0 { // a very first contact or crashed one
		if vs.view == nil {
			// the first one becomes the primary
			vs.view = createView(1, server, "")
		} else {
			// restarted primary is treated as dead
			if server == vs.view.Primary {
				vs.pttl = 0;  // set primary dead
				if (vs.acked && vs.switchView()) {
					vs.acked = false
				}
			}

			// register extra servers - assume it is a trusted server
			if server != "" && server != vs.view.Backup {
				vs.idleServers[server] = DeadPings
			}
		}
	} else {
		if server == vs.view.Primary {
			if viewno == vs.view.Viewnum {
				// the primary is correctly acked
				vs.acked = true
			}
		}
	}

	// update TTLs to DeadPings
	if server == vs.view.Primary {
		vs.pttl = DeadPings
	} else if server == vs.view.Backup {
		vs.bttl = DeadPings
	} else {
		vs.idleServers[server] = DeadPings
	}

	reply.View = *vs.view
	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	if vs.view == nil {
		reply.View = *createView(0, "", "")
	} else {
		reply.View = *vs.view
	}
	return nil
}

func (vs *ViewServer) prepareNewview(primary string, backup string) {
	if vs.view == nil {
		return
	}

	if vs.newv == nil {
		vs.newv = createView(vs.view.Viewnum + 1, primary, backup)
	} else {
		vs.newv.Primary = primary
		vs.newv.Backup = backup
	}
}

func promoteToBackup(idleServers *map[string]int) string {
	for server := range *idleServers {
		delete(*idleServers, server)
		return server
	}
	return ""
}

//
// The view service proceeds to a new view 
//     1) if it hasn't received recent Pings > DeadPings from both primary and backup
//     2) if the primary or backup crashed and restarted
//     3) if there is no backup and there is an idle server
//
// This function should be called ONLY IF the primary has acked the current view
//
func (vs *ViewServer) switchView() bool {
	view := vs.view

	// no candidate server
	if view.Backup == "" && len(vs.idleServers) == 0 {
		return false
	}

	if vs.pttl > 0 && vs.bttl <= 0 {
		// case 2/3: there is no backup or the backup is dead
		vs.prepareNewview(view.Primary, promoteToBackup(&vs.idleServers))
	} else if vs.pttl <= 0 && vs.bttl > 0 {
		// case 2: the primary crashed or restarted
		vs.prepareNewview(view.Backup, promoteToBackup(&vs.idleServers))
	} else if vs.pttl <= 0 && vs.bttl <= 0 {
		// case 1: both primary and backup are dead in current view
		vs.prepareNewview("", "")
	}

	// do the switch
        if vs.newv != nil {
                vs.view, vs.newv = vs.newv, nil
                return true
        }
        return false
}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	if vs.view == nil {
		return
	}

	// clean idle servers (died)
        for server := range vs.idleServers {
                if vs.idleServers[server] <= 0 {
                        delete(vs.idleServers, server)
                } else {
                        vs.idleServers[server]--
                }
        }

	// if the primary has acked current view, try to switch to the new view
	if (vs.acked && vs.switchView()) {
		vs.acked = false
	}

	// if no primary, set primary ""
	if vs.view.Primary == "" {
		vs.pttl = 0
	}

	// if no backup or a backup has been promoted to primary, set backup ""
	if vs.view.Backup == "" {
		vs.bttl = 0
	}

	// decrease ttl in every tick
	if vs.pttl > 0 {
		vs.pttl--
	}
	if vs.bttl > 0 {
		vs.bttl--
	}
}

//
// tell the server to shut itself down.
// for testing.
//
func (vs *ViewServer) Kill() {
	atomic.StoreInt32(&vs.dead, 1)
	vs.l.Close()
}

//
// has this server been asked to shut down?
//
func (vs *ViewServer) isdead() bool {
	return atomic.LoadInt32(&vs.dead) != 0
}

//
// for testing.
//
func (vs *ViewServer) GetRPCCount() int32 {
	return atomic.LoadInt32(&vs.rpccount)
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	vs.idleServers = make(map[string] int)

	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.isdead() == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.isdead() == false {
				atomic.AddInt32(&vs.rpccount, 1)
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.isdead() == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.isdead() == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
