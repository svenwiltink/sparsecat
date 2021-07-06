## SparseCat

### Goal
Skipping the hole in sparse file when transmitting large files over the network. Using the filesystem seek capabilities
hole can be detected. Instead of transmitting these zero bytes and wasting precious bandwidth only sections of the file
containing data are sent.


### Example usage
```shell
// create sparse image
truncate -s150G image.raw

// add some random data to the sparse file
dd if=/dev/urandom bs=4M count=10 conv=notrunc seek=30 of=image.raw

// send sparse file and reconstruct it on the other host. The amount
// of data transmitted will only be 40MB instead of 150G
sparsecat -if image.raw | pv | ssh GLaDOS sparsecat -r -of image.raw
```
[![asciicast](https://asciinema.org/a/BMQStO5yWGWsG3xBigE2NV9Gx.svg)](https://asciinema.org/a/BMQStO5yWGWsG3xBigE2NV9Gx)

### But how does it work?

Sparsecat used the `SEEK_HOLE` and `SEEK_DATA` capabilities of `lseek` on linux. See [the man pages](https://man7.org/linux/man-pages/man2/lseek.2.html)
for more information. Before sending the data inside a file Sparsecat creates a small header containing the size of the
source file. The data sections follow this header. Each section consists of the offset in the target file, the length
of the data section followed by the data itself. The wire format is identical to [ceph rbd export-diff](https://github.com/ceph/ceph/blob/aa913ced1240a366e063182cd359b562c626643d/doc/dev/rbd-diff.rst)


When receiving a Sparsecat stream the Decoder detects if the target is an `*os.File`. When this is the case and the
file is capable of seeking a fast path is used and the sparseness of the target file is preserved. When the target
is not a file, such as an `io.Copy` to a buffer, Sparsecat will pad the output zero bytes. As if it is outputting the
entire file.