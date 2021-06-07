## SparseCat

### Goal
Transmitting sparse files using minimal amount of network bandwidth. Sparsecat 
uses the SEEK_HOLE and SEEK_DATA capabilities of unix filesystems to find holes
in sparse files and only transmits sections containing data. The wire format
is simple and compatible with the ceph rbd diff v1 format as described [here](https://github.com/ceph/ceph/blob/aa913ced1240a366e063182cd359b562c626643d/doc/dev/rbd-diff.rst)

[![asciicast](https://asciinema.org/a/BMQStO5yWGWsG3xBigE2NV9Gx.svg)](https://asciinema.org/a/BMQStO5yWGWsG3xBigE2NV9Gx)

### Support

Because the tool relies on the `lseek` syscall with `SEEK_HOLE` and `SEEK_DATA`
only unix systems with the correct filesystems are supported. See [the man pages](https://man7.org/linux/man-pages/man2/lseek.2.html)
for more information. `sparsecat` does work with unsupported filesystems, but it 
will simply transmit the entire file with a couple of bytes of overhead.