import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @Project: breakpoint
 * @Description:
 * @Author: Cenjie
 * @Date: Created in 2019/2/11
 */
public class Downloader {
    private String urlStr;
    private int threadNum;
    private String filename;
    private String filename_tmp;
    private CountDownLatch latch;
    private long fileLength;
    private long lenPerThread;  //每个线程的下载大小
    private long[] start;    //保留每个线程下载的起始位置。
    private long[] end;      //保留每个线程下载的结束位置。
    private URL url;

    public Downloader(String urlStr, int threadNum) {
        this.urlStr = urlStr;
        this.threadNum = threadNum;
        start = new long[this.threadNum];
        end = new long[this.threadNum];
        latch = new CountDownLatch(this.threadNum);
    }

    /**
     * 文件下载
     */
    public void download() {
        File file = null;
        File file_tmp = null;

        //从文件链接中获取文件名
        filename = urlStr.substring(urlStr.lastIndexOf('/') + 1, urlStr
                .contains("?") ? urlStr.lastIndexOf('?') : urlStr.length());
        //设置临时文件的文件名
        filename_tmp = filename + "_tmp";

        try {
            //创建url
            url = new URL(urlStr);

            //打开下载链接，并且得到一个HttpURLConnection的一个对象httpcon
            HttpURLConnection httpcon = (HttpURLConnection) url.openConnection();
            httpcon.setRequestMethod("GET");

            //获取请求资源的总长度，为Long型
            fileLength = httpcon.getContentLengthLong();

            //下载文件和临时文件
            file = new File(filename);
            file_tmp = new File(filename_tmp);

            //每个线程需下载的资源大小；由于文件大小不确定，为避免数据丢失
            lenPerThread = fileLength % threadNum == 0 ? fileLength / threadNum : fileLength / threadNum + 1;
            //打印下载信息
            System.out.println("文件名: " + filename + "，" + "文件大小："
                    + fileLength + "字节，每个线程下载大小：" + lenPerThread + "字节");

            if (file.exists() && file.length() == fileLength) {
                System.out.println("文件已存在");
                return;
            } else {
                setBreakPoint(file_tmp);
                ExecutorService exec = Executors.newCachedThreadPool();
                for (int i = 0; i < threadNum; i++) {
                    exec.execute(new DownLoadThread(start[i], end[i],
                            this, i));
                }
                latch.await();  //当所有线程下载完毕后，才会从此阻塞中返回
                System.out.println("文件下载完成");
                exec.shutdown();
            }
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        //下载完成后，判断文件是否完整，并删除临时文件
        if (file.length() == fileLength) {
            if (file_tmp.exists()) {
                file_tmp.delete();
                System.out.println("删除临时文件完成，下载结束");
            }
        } else{
            System.out.println("该文件不完整");
        }
    }

    /**
     * 读取临时文件中记录的断点，加载每个线程的任务区间，若临时文件不存在，则重新分配每个线程的任务区间
     * @param file_tmp
     */
    private void setBreakPoint(File file_tmp) {
        RandomAccessFile random_file_tmp = null;
        System.out.println("开始分配任务区间：");
        try {
            //如果存在临时文件，则从临时文件记录的位置继续下载
            if (file_tmp.exists()) {
                System.out.println("找到临时文件，将从断点处恢复下载...");
                random_file_tmp = new RandomAccessFile(file_tmp, "rw");
                for (int i = 0; i < threadNum; i++) {
                    random_file_tmp.seek(i * 8);
                    start[i] = random_file_tmp.readLong();

                    random_file_tmp.seek(1000 + i * 8);
                    end[i] = random_file_tmp.readLong();

                    System.out.println("线程" + i + " 起始位置："
                            + start[i] + "，结束位置：" + end[i]);
                }
            } else {
                System.out.println("未找到临时文件，开始一个新的下载...");
                random_file_tmp = new RandomAccessFile(file_tmp, "rw");

                for (int i = 0; i < threadNum; i++) {
                    //设置线程i的下载起始位置
                    start[i] = lenPerThread * i;
                    if (i == threadNum - 1) {
                        //当线程i为最后一个线程时，设置线程i的下载结束位置为文件长度
                        end[i] = fileLength - 1;
                    } else {
                        end[i] = lenPerThread * (i + 1) - 1;
                    }

                    random_file_tmp.seek(i * 8);
                    random_file_tmp.writeLong(start[i]);

                    random_file_tmp.seek(1000 + i * 8);
                    random_file_tmp.writeLong(end[i]);

                    System.out.println("线程" + i + " 起始位置："
                            + start[i] + "，结束位置：" + end[i]);
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (random_file_tmp != null) {
                    random_file_tmp.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    class DownLoadThread implements Runnable {
        private int id;         //线程id
        private long startPos;  //线程下载起始位置
        private long endPos;    //线程下载结束位置
        private Downloader task;
        private RandomAccessFile rand_file;
        private RandomAccessFile rand_file_tmp;

        public DownLoadThread(long startPos, long endPos,
                              Downloader task, int id) {
            this.startPos = startPos;
            this.endPos = endPos;
            this.task = task;
            this.id = id;
            try {
                this.rand_file = new RandomAccessFile(this.task.filename, "rw");
                this.rand_file_tmp = new RandomAccessFile(this.task.filename_tmp, "rw");
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }

        public void run() {
            HttpURLConnection httpcon;
            InputStream is = null;
            int length;

            System.out.println("线程" + id + " 开始下载...");

            while (true) {
                try {
                    httpcon = (HttpURLConnection) task.url.openConnection();
                    httpcon.setRequestMethod("GET");

                    //防止网络阻塞，设置指定的超时时间；单位都是ms。超过指定时间，就会抛出异常
                    httpcon.setReadTimeout(20000);//读取数据的超时设置
                    httpcon.setConnectTimeout(20000);//连接的超时设置

                    if (startPos < endPos) {

                        //向服务器请求指定区间段的数据，这是实现断点续传的根本。
                        httpcon.setRequestProperty("Range", "bytes=" + startPos + "-" + endPos);

                        System.out.println("线程" + id + " 长度：" + (endPos - startPos + 1));

                        rand_file.seek(startPos);

                        is = httpcon.getInputStream();//获取服务器返回的资源流
                        long count = 0L;
                        byte[] buf = new byte[1024];

                        while ((length = is.read(buf)) != -1) {
                            count += length;
                            rand_file.write(buf, 0, length);

                            //不断更新每个线程下载资源的起始位置，并写入临时文件
                            startPos += length;
                            rand_file_tmp.seek(id * 8);
                            rand_file_tmp.writeLong(startPos);
                        }
                        System.out.println("线程" + id
                                + " 总下载大小: " + count);

                        //关闭流
                        is.close();
                        httpcon.disconnect();
                        rand_file.close();
                        rand_file_tmp.close();
                    }
                    latch.countDown();
                    System.out.println("线程" + id + " 下载完成");
                    break;
                } catch (IOException e) {
                    e.printStackTrace();
                } finally {
                    try {
                        if (is != null) {
                            is.close();
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    public static void main(String[] args) {
        int threadNum = 10;
        String url = "https://github.com/cenjieHo/cenjieHo.github.io.git";

        Downloader load = new Downloader(url, threadNum);
        load.download();
    }
}
