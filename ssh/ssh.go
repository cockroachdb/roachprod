package ssh

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"path"
	"path/filepath"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/cockroachdb/roachprod/config"
	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/agent"
	"golang.org/x/crypto/ssh/knownhosts"
)

var knownHosts ssh.HostKeyCallback
var knownHostsOnce sync.Once
var InsecureIgnoreHostKey bool

func getKnownHosts() ssh.HostKeyCallback {
	knownHostsOnce.Do(func() {
		var err error
		if InsecureIgnoreHostKey {
			knownHosts = ssh.InsecureIgnoreHostKey()
		} else {
			knownHosts, err = knownhosts.New(filepath.Join(os.Getenv("HOME"), ".ssh", "known_hosts"))
			if err != nil {
				log.Fatal(err)
			}
		}
	})
	return knownHosts
}

func getSSHAgentSigners() []ssh.Signer {
	const authSockEnv = "SSH_AUTH_SOCK"
	agentSocket := os.Getenv(authSockEnv)
	if agentSocket == "" {
		return nil
	}
	sock, err := net.Dial("unix", agentSocket)
	if err != nil {
		log.Printf("SSH_AUTH_SOCK set but unable to connect to agent: %s", err)
		return nil
	}
	agent := agent.NewClient(sock)
	signers, err := agent.Signers()
	if err != nil {
		log.Printf("unable to retrieve keys from agent: %s", err)
		return nil
	}
	return signers
}

func getSSHKeySigner(path string) ssh.Signer {
	key, err := ioutil.ReadFile(path)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Printf("unable to read SSH key %q: %s", path, err)
		}
		return nil
	}

	signer, err := ssh.ParsePrivateKey(key)
	if err != nil {
		log.Printf("unable to parse SSH key %q: %s", path, err)
		return nil
	}
	return signer
}

func getDefaultSSHKeySigners() []ssh.Signer {
	return []ssh.Signer{
		getSSHKeySigner(filepath.Join(config.OSUser.HomeDir, ".ssh", "id_rsa")),
		getSSHKeySigner(filepath.Join(config.OSUser.HomeDir, ".ssh", "google_compute_engine")),
	}
}

func newSSHClient(user, host string) (*ssh.Client, net.Conn, error) {
	config := &ssh.ClientConfig{
		User:            user,
		Auth:            []ssh.AuthMethod{ssh.PublicKeys(sshState.signers...)},
		HostKeyCallback: getKnownHosts(),
	}
	config.SetDefaults()

	addr := fmt.Sprintf("%s:22", host)
	conn, err := net.DialTimeout("tcp", addr, 30*time.Second)
	if err != nil {
		return nil, nil, err
	}
	c, chans, reqs, err := ssh.NewClientConn(conn, addr, config)
	if err != nil {
		return nil, nil, err
	}
	return ssh.NewClient(c, chans, reqs), conn, nil
}

type sshClient struct {
	sync.Mutex
	*ssh.Client
}

var sshState = struct {
	signers     []ssh.Signer
	signersInit sync.Once

	clients  map[string]*sshClient
	clientMu sync.Mutex
}{
	clients: map[string]*sshClient{},
}

func NewSSHSession(user, host string) (*ssh.Session, error) {
	sshState.clientMu.Lock()
	target := fmt.Sprintf("%s@%s", user, host)
	client := sshState.clients[target]
	if client == nil {
		client = &sshClient{}
		sshState.clients[target] = client
	}
	sshState.clientMu.Unlock()

	sshState.signersInit.Do(func() {
		sshState.signers = append(sshState.signers, getSSHAgentSigners()...)
		sshState.signers = append(sshState.signers, getDefaultSSHKeySigners()...)
	})

	client.Lock()
	defer client.Unlock()
	if client.Client == nil {
		var err error
		client.Client, _, err = newSSHClient(user, host)
		if err != nil {
			return nil, err
		}
	}
	return client.NewSession()
}

func IsSigKill(err error) bool {
	switch t := err.(type) {
	case *ssh.ExitError:
		return t.Signal() == string(ssh.SIGKILL)
	}
	return false
}

type progressWriter struct {
	writer   io.Writer
	done     int64
	total    int64
	progress func(float64)
}

func (p *progressWriter) Write(b []byte) (int, error) {
	n, err := p.writer.Write(b)
	if err == nil {
		p.done += int64(n)
		p.progress(float64(p.done) / float64(p.total))
	}
	return n, err
}

func SCPPut(src, dest string, progress func(float64), session *ssh.Session) error {
	f, err := os.Open(src)
	if err != nil {
		return err
	}
	defer f.Close()
	s, err := f.Stat()
	if err != nil {
		return err
	}

	errCh := make(chan error, 1)
	go func() {
		w, err := session.StdinPipe()
		if err != nil {
			errCh <- err
			return
		}
		defer w.Close()
		fmt.Fprintf(w, "C%#o %d %s\n", s.Mode().Perm(), s.Size(), path.Base(src))
		p := &progressWriter{w, 0, s.Size(), progress}
		if _, err := io.Copy(p, f); err != nil {
			errCh <- err
			return
		}
		fmt.Fprint(w, "\x00")
		close(errCh)
	}()

	err = session.Run(fmt.Sprintf("rm -f %s ; scp -t %s", dest, dest))
	select {
	case err := <-errCh:
		return err
	default:
		return err
	}
}

// TODO(benesch): Make progress handling for directories less confusing. The
// SCP protocol makes this challenging, as it does not send the total size of
// all files.
func SCPGet(src, dest string, progress func(float64), session *ssh.Session) error {
	errCh := make(chan error, 1)
	go func() {
		rp, err := session.StdoutPipe()
		if err != nil {
			errCh <- err
			return
		}
		wp, err := session.StdinPipe()
		if err != nil {
			errCh <- err
			return
		}
		defer wp.Close()

		r := bufio.NewReader(rp)
		dirStack := []string{}
		for {
			fmt.Fprint(wp, "\x00")

			line, err := r.ReadBytes('\n')
			if err != nil {
				if len(dirStack) != 0 || err != io.EOF {
					errCh <- err
				}
				close(errCh)
				return
			}

			if line[0] == 'E' {
				dirStack = dirStack[:len(dirStack)-1]
				continue
			}

			var op byte
			var mode uint32
			var size int64
			var name string
			if n, err := fmt.Sscanf(string(line), "%c%o %d %s", &op, &mode, &size, &name); err != nil {
				errCh <- fmt.Errorf("decoding scp directive: %s: %q", err, line)
				return
			} else if n != 4 {
				errCh <- errors.New(string(line))
				return
			}

			fullname := dest
			if len(dirStack) > 0 {
				fullname = filepath.Join(append(dirStack, name)...)
			}

			switch op {
			case 'C':
				f, err := os.Create(fullname)
				if err != nil {
					errCh <- err
					return
				}
				defer f.Close()

				if err := f.Chmod(os.FileMode(mode)); err != nil {
					errCh <- err
					return
				}

				fmt.Fprint(wp, "\x00")

				p := &progressWriter{f, 0, size, progress}
				if _, err := io.Copy(p, io.LimitReader(r, size)); err != nil {
					errCh <- err
					return
				}

				// File data has a trailing null byte.
				_, err = r.ReadByte()
				if err != nil {
					errCh <- err
					return
				}
			case 'D':
				if err := os.Mkdir(fullname, os.FileMode(mode)); err != nil && !os.IsExist(err) {
					errCh <- err
					return
				}
				if len(dirStack) == 0 {
					dirStack = append(dirStack, dest)
				} else {
					dirStack = append(dirStack, name)
				}
			default:
				errCh <- fmt.Errorf("unknown operation '%c'", op)
				return
			}
		}
	}()

	err := session.Run(fmt.Sprintf("scp -qrf %s", src))
	select {
	case err := <-errCh:
		return err
	default:
		return err
	}
}
