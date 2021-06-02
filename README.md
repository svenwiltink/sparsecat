## SparseCat

WIP command line tool to `cat` sparse files into a simple
binary format. The format is to be determined, but will be similar
to the ceph image format for compatibility.

The goal of the tool is to only output file section that contain data,
omitting sparse holes. The binary output will contain the file offset
followed by the length of the data and the data itself.

A tool recieving the data stream will be able to reconstruct
the file by allocating a sparse file of the correct size and writing
the transmitted file sections.


### Support

Because the tool relies on the `lseek` syscall with `SEEK_HOLE` and `SEEK_DATA`
only unix systems with the correct filesystems are supported. See [the man pages](https://man7.org/linux/man-pages/man2/lseek.2.html)
for more information.