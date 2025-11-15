package ca.concordia.filesystem;

import ca.concordia.filesystem.datastructures.FEntry;

import java.io.RandomAccessFile;
import java.util.concurrent.locks.ReentrantLock;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;



public class FileSystemManager {

    private final int MAXFILES = 5;
    private final int MAXBLOCKS = 10;
    private  final static FileSystemManager instance = null;
    private final RandomAccessFile disk;
    private final ReentrantLock globalLock = new ReentrantLock();

    private static final int BLOCK_SIZE = 128; // Example block size

    private FEntry[] inodeTable; // Array of inodes
    private boolean[] freeBlockList; // Bitmap for free blocks

    public FileSystemManager(String filename, int totalSize) {
        // Initialize the file system manager with a file
        if(instance == null) {
            
            try {
                disk = new RandomAccessFile(filename, "rw");
            } catch (FileNotFoundException e) {
                throw new RuntimeException("Unable to open disk file: " + filename, e);
            }

           inodeTable = new FEntry[MAXFILES]; // track existing files
           freeBlockList = new boolean[MAXBLOCKS]; // track free blocks

            // freeing all blocks
           try {
                loadMetadata();
           } catch (Exception e){
              for (int i = 0; i < MAXBLOCKS; i++) {
                freeBlockList[i] = true;
              }
           }


        } else {
            throw new IllegalStateException("FileSystemManager is already initialized.");
        }

    }

    public void createFile(String fileName) throws Exception {
        globalLock.lock();  // prevent race conditions
        try {
            if (fileName == null || fileName.length() > 11) {
                throw new Exception("ERROR: filename too large");
            }

            for (int i = 0; i < MAXFILES; i++) {
                //cheking for dups
                if (inodeTable[i] != null && inodeTable[i].getFilename().equals(fileName)) {
                    System.out.println("File " + fileName + " already exists");
                    return;
                }
                //adding fEntry to free slot
                if(inodeTable[i] == null) {
                    inodeTable[i] = new FEntry(fileName,(short)0,(short)-1);
                    System.out.println("File " + fileName + " created");
                    return;
                }
            }
            throw new Exception("ERROR: exceeded maximum file count ");
        } finally {
            saveMetadata();
            globalLock.unlock();
        }

    }

    public void writeFile(String fileName, byte[] data) throws Exception {
        globalLock.lock();  // prevent race conditions
        try {

            // # of blocks needed for file
            int nBlocks = (int) Math.ceil((double)data.length/BLOCK_SIZE);
            if (nBlocks == 0) {
                throw new Exception("ERROR: data is empty");
            }
            if (nBlocks > MAXBLOCKS) {
                throw new Exception("ERROR: File too large");
            }

            // creates index + check if file exists
            int fileIdx = getFileIdx(fileName);
            if(fileIdx == -1) {
                throw new Exception("ERROR: file does not exist");
            }

            FEntry file = inodeTable[fileIdx];

            // prepare a temporary free map that includes the file's current blocks
            boolean[] freeSnapshot = Arrays.copyOf(freeBlockList, freeBlockList.length);
            short oldFirst = file.getFirstBlock();
            int oldBlocks = (int) Math.ceil((double)file.getFilesize() / BLOCK_SIZE);
            if (oldFirst >= 0 && oldBlocks > 0) {
                for (int i = 0; i < oldBlocks; i++) {
                    int idx = oldFirst + i;
                    if (idx >= 0 && idx < freeSnapshot.length) {
                        freeSnapshot[idx] = true;
                    }
                }
            }

            int startBlock = findFreeBlockRange(nBlocks, freeSnapshot);
            if (startBlock == -1) {
                throw new Exception("ERROR: Not enough  blocks for file size");
            }

            // mark blocks as used on snapshot, then commit to shared state
            for (int i = 0; i < nBlocks; i++) {
                freeSnapshot[startBlock + i] = false;
            }
            System.arraycopy(freeSnapshot, 0, freeBlockList, 0, freeBlockList.length);

            for(int i = 0; i < nBlocks; i++) {
                //find, allocate, write data
                disk.seek((long) (startBlock + i) * BLOCK_SIZE);
                int start = i * BLOCK_SIZE;
                int end = Math.min(start + BLOCK_SIZE, data.length);
                disk.write(data,start, end - start);
            }

            file.setFilesize((short)data.length);
            file.setFirstBlock((short)startBlock);
            System.out.println("Wrote " + data.length + " bytes to " + fileName);

        } finally {
            saveMetadata();
            globalLock.unlock();
        }
    }

    public byte[] readFile(String fileName) throws Exception {
        globalLock.lock();
        try {
            // find file with name 
            int fIndex = getFileIdx(fileName);
            if (fIndex == -1) throw new Exception("ERROR: file does not exist");

            // get file object
            FEntry file = inodeTable[fIndex];
            if (file.getFirstBlock() < 0) {
                throw new Exception("ERROR: file has no data");
            }
            byte[] data = new byte[file.getFilesize()];

            // find, allocate, read data (assumes contiguous blocks)
            int nBlocks = (int) Math.ceil((double)file.getFilesize() / BLOCK_SIZE);
            int bytesRead = 0;
            for (int i = 0; i < nBlocks; i++) {
                disk.seek((long) (file.getFirstBlock() + i) * BLOCK_SIZE);
                int bytesToRead = Math.min(BLOCK_SIZE, data.length - bytesRead);
                disk.read(data, bytesRead, bytesToRead);
                bytesRead += bytesToRead;
            }

            // return data
            return data;
        } finally {
            globalLock.unlock();
        }
    }

    public void deleteFile(String fileName) throws Exception {
        globalLock.lock();
        try {

            int index = getFileIdx(fileName);
            if (index == -1) throw new Exception("ERROR: file does not exist");

            FEntry file = inodeTable[index];
            // free blocks associated with this file
            short firstBlock = file.getFirstBlock();
            int nBlocks = (int) Math.ceil((double)file.getFilesize() / BLOCK_SIZE);
            if(firstBlock != -1 && (firstBlock >= 0 && firstBlock < MAXBLOCKS) && nBlocks > 0) {
                markBlockRangeUsed(firstBlock, nBlocks, false);
            }

            //remove object at file index
            inodeTable[index] = null;
            System.out.println("File deleted: " + fileName);
        } finally {
            saveMetadata();
            globalLock.unlock();
        }
    }

    public String listFiles() {
        globalLock.lock();
        try {
            ArrayList<String> files = new ArrayList<>();

            for (FEntry entry : inodeTable) {
                if (entry != null) {
                    files.add(entry.getFilename());
                }
            }


            if (files.isEmpty()) {
                return "No files";
            }

            return String.join(", ", files);
        } finally {
            globalLock.unlock();
        }
    }



    //helpers
    private int getFileIdx(String fileName) {
        if (fileName == null) {
            return -1;
        }
        for (int i = 0;i < MAXFILES ;i++ ) {
            if(inodeTable[i] !=null && inodeTable[i].getFilename().equals(fileName)) {
                return i;
            }
        }
        return -1;
    }
    private int findFreeBlock() {
        for (int i = 0;i < MAXBLOCKS ;i++ ) {
            if (freeBlockList[i]) {
                freeBlockList[i] = false;
                return i;
            }
        }
        return -1;
    }

    // Find a contiguous run of free blocks of the requested length. Returns start index or -1.
    private int findFreeBlockRange(int length) {
        return findFreeBlockRange(length, freeBlockList);
    }

    private int findFreeBlockRange(int length, boolean[] freeMap) {
        if (length <= 0 || length > MAXBLOCKS) {
            return -1;
        }
        for (int start = 0; start <= MAXBLOCKS - length; start++) {
            boolean allFree = true;
            for (int offset = 0; offset < length; offset++) {
                if (!freeMap[start + offset]) {
                    allFree = false;
                    break;
                }
            }
            if (allFree) {
                return start;
            }
        }
        return -1;
    }

    private void markBlockRangeUsed(int start, int length, boolean used) {
        for (int offset = 0; offset < length; offset++) {
            int idx = start + offset;
            if (idx < 0 || idx >= MAXBLOCKS) {
                throw new IllegalArgumentException("Block index out of range");
            }
            freeBlockList[idx] = !used;
        }
    }
    private void loadMetadata() throws Exception {
    disk.seek(0);

    for (int i = 0; i < MAXFILES; i++) {
        boolean exists = disk.readBoolean();
        if (!exists) {
            inodeTable[i] = null;
        } else {
            String name = disk.readUTF();
            short size = disk.readShort();
            short first = disk.readShort();
            inodeTable[i] = new FEntry(name, size, first);
        }
    }

    for (int i = 0; i < MAXBLOCKS; i++) {
        freeBlockList[i] = disk.readBoolean();
    }
  }
  private void saveMetadata() throws Exception {
    disk.seek(0);

    // Save inodeTable
    for (int i = 0; i < MAXFILES; i++) {
        FEntry entry = inodeTable[i];
        if (entry == null) {
            disk.writeBoolean(false);
        } else {
            disk.writeBoolean(true);
            disk.writeUTF(entry.getFilename());
            disk.writeShort(entry.getFilesize());
            disk.writeShort(entry.getFirstBlock());
        }
    }

    // Save freeBlockList
    for (int i = 0; i < MAXBLOCKS; i++) {
        disk.writeBoolean(freeBlockList[i]);
    }
  }




}
