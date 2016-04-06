import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Random;
import java.util.Scanner;
import java.util.TreeMap;

/**
 * The class is node in the random tree structure.
 * 
 * @author Akshai Prabhu
 *
 */
public class Node {

	// data structure to hold hashing table
	public static String hashTable[][] = { { "glados", "129.21.22.196" },
			{ "rhea", "129.21.37.49" }, { "kansas", "129.21.37.18" },
			{ "newyork", "129.21.37.16" }, { "california", "129.21.37.23" },
			{ "nevada", "129.21.37.25" }, { "nebraska", "129.21.37.9" },
			{ "utah", "129.21.37.11" }, { "arizona", "129.21.37.15" },
			{ "missouri", "129.21.37.8" } };
	public static String currentFileName = new String();
	public static String myIP = new String();
	public static TreeMap<String, Integer> counterTable = new TreeMap<String, Integer>();

	/**
	 * The client part of the node.
	 * 
	 * @author Akshai Prabhu
	 *
	 */
	public class Client extends Thread {
		/**
		 * Thread run method
		 */
		public void run() {
			while (true) {
				System.out.println("To insert file, press 0 and press enter");
				System.out.println("To search file, press 1 and press enter");
				Scanner sc = new Scanner(System.in);
				String choice = sc.nextLine();
				System.out.println("Input the file name and press enter");
				String fileName = sc.nextLine();
				if (choice.trim().equals("1")) {
					findFile(fileName);
					currentFileName = fileName;
				} else if (choice.trim().equals("0")) {
					insertFile(fileName);
				} else {
					System.out.println("Invalid choice!!!");
				}
				// sc.close();
			}
		}

		/**
		 * To insert a file into the random tree root node
		 * 
		 * @param fileName
		 */
		public void insertFile(String fileName) {
			int hashIndex = getHash(fileName, 0, 0);
			System.out.println(hashIndex + "---");
			File file = new File(fileName);
			if (!file.exists()) {
				System.out.println("No such file!!!");
				System.exit(0);
			} else {
				Socket socket;
				try {
					socket = new Socket(hashTable[hashIndex][1], 40000);
					OutputStream outToServer = socket.getOutputStream();
					DataOutputStream out = new DataOutputStream(outToServer);
					out.writeUTF("Insert file: " + file);
					System.out.println("Inserting file");
					Thread.sleep(2000);
					socket.close();
					Thread.sleep(2000);
					socket = new Socket(hashTable[hashIndex][1], 50000);
					FileInputStream fis = new FileInputStream(file);
					BufferedInputStream bis = new BufferedInputStream(fis);

					OutputStream os = socket.getOutputStream();

					byte byteArray[];
					long count = 0;

					while (count != file.length()) {
						int n = 10000;
						if (file.length() - count >= n) {
							count += n;
						} else {
							n = (int) (file.length() - count);
							count = file.length();
						}
						byteArray = new byte[n];
						bis.read(byteArray, 0, n);
						os.write(byteArray);
					}
					os.flush();
					os.close();
					bis.close();
					socket.close();
				} catch (IOException | InterruptedException e) {
					e.printStackTrace();
				}
			}
		}

		/**
		 * To search for a file in the random tree
		 * 
		 * @param filename
		 */
		public void findFile(String filename) {
			Random ran = new Random();
			int child = ran.nextInt(4);
			int hashIndex = getHash(filename, 2, child);
			try {
				sendRequest(InetAddress.getLocalHost().getHostName(), filename,
						hashIndex, 2, child, "");
			} catch (UnknownHostException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		/**
		 * To send a request to search a file from one node to another in the
		 * random tree
		 * 
		 * @param originalRequester
		 * @param fileName
		 * @param hashIndex
		 * @param i
		 * @param j
		 * @param trail
		 */
		public void sendRequest(String originalRequester, String fileName,
				int hashIndex, int i, int j, String trail) {
			String serverName = new String();
			serverName = hashTable[hashIndex][1];
			Socket socket;
			try {
				socket = new Socket(serverName, 40000);
				OutputStream outToServer = socket.getOutputStream();
				DataOutputStream out = new DataOutputStream(outToServer);
				System.out.println("Requesting: " + fileName);
				trail += InetAddress.getLocalHost().getHostName();
				out.writeUTF(originalRequester + "=== Requesting file: "
						+ fileName + " , " + hashIndex + " @ " + i + " * " + j
						+ " $ " + trail);

				socket.close();
			} catch (IOException e) {
				e.printStackTrace();
			}

		}

		public int getHash(String filename, int i, int j) {
			return Math.abs((filename + i + j).hashCode()) % 10;
		}
	}

	/**
	 * The class listens to and handles all incoming messages or requests in the
	 * random tree
	 * 
	 * @author Akshai Prabhu
	 *
	 */
	public class RequestServer extends Thread {
		ServerSocket requestServerSocket;
		ServerSocket fileServerSocket;
		FileServer fileServer = new FileServer();

		/**
		 * Thread run method
		 */
		public void run() {
			serverListen();
		}

		/**
		 * The server listener
		 */
		public void serverListen() {
			Socket socket;
			String message = new String();
			while (true) {
				try {
					requestServerSocket = new ServerSocket(40000);
					socket = requestServerSocket.accept();
					String inetAddress = socket.getInetAddress().toString();
					DataInputStream in = new DataInputStream(
							socket.getInputStream());
					message = in.readUTF();
					socket.close();
					requestServerSocket.close();
					if (message.equals("File not found!!!")) {// file not found
																// message
						System.out.println(message);
					} else if (message.contains("Trail")) {// trail message
						System.out.println(message);
					} else if (message.contains("Insert")) {// insert message
						System.out.println(message);
						currentFileName = message.substring(message
								.indexOf(":") + 2);
						System.out.println("---" + currentFileName);
					} else if (message.contains("Replicate")) {// replicate file
																// message
						currentFileName = message.substring(message
								.indexOf(":") + 2);
						counterTable.remove(currentFileName);
						System.out.println(currentFileName
								+ " is replicated!!!");
					} else {
						System.out.println(message);
						Thread.sleep(2000);
						String fileName = message.substring(
								message.indexOf(":") + 2, message.indexOf(","));
						fileName = fileName.trim();
						String myIndex = message.substring(
								message.indexOf(",") + 2, message.indexOf("@"));
						myIndex = myIndex.trim();
						String level = message.substring(
								message.indexOf("@") + 2, message.indexOf("*"));
						level = level.trim();
						String indexAtLevel = message.substring(
								message.indexOf("*") + 2, message.indexOf("$"));
						indexAtLevel = indexAtLevel.trim();
						String trail = message.substring(
								message.indexOf("$") + 2, message.length());
						trail = trail.trim() + "---";
						String originalRequestor = message.substring(0,
								message.indexOf("="));

						originalRequestor = originalRequestor.trim();

						System.out.println(fileName);
						System.out.println(myIndex);
						System.out.println(inetAddress);

						RequestServer myRequestServer = new RequestServer();
						System.out.println("Before file found...");
						Thread.sleep(1000);
						if (fileLookUp(fileName, myIndex)) {
							System.out.println("File found...");
							Thread.sleep(2000);
							System.out.println("sending file...");
							fileServer.sendFile(fileName, myIndex,
									originalRequestor);

							Thread.sleep(2000);
							System.out.println("sending trail...");
							trail += InetAddress.getLocalHost().getHostName();
							myRequestServer.sendTrail(originalRequestor, trail);
							Thread.sleep(2000);
							if (!counterTable.containsKey(fileName)
									&& (level.equals("0") || level.equals("1"))) {
								counterTable.put(fileName, 1);
							} else if (level.equals("0") || level.equals("1")) {
								counterTable.replace(fileName,
										(counterTable.get(fileName)) + 1);
							}
							if ((level.equals("0") || level.equals("1"))
									&& (!counterTable.isEmpty() || counterTable != null)) {
								if (counterTable.get(fileName) == 5) {
									replicateFile(fileName, level, myIndex,
											indexAtLevel);
								}
							}
						} else {
							Client myClient = new Client();
							int i = Integer.parseInt(level);
							int j = Integer.parseInt(indexAtLevel);
							if (j == 2 || j == 3) {
								j = 1;
							} else {
								j = 0;
							}
							i -= 1;
							if (i < 0) {
								sendFileNotFound(originalRequestor);
							} else {
								int hashIndex = myClient
										.getHash(fileName, i, j);
								myClient.sendRequest(originalRequestor,
										fileName, hashIndex, i, j, trail);
							}
						}
					}

				} catch (InterruptedException | IOException e) {
					System.out.println(e.getClass().getName());
					System.exit(0);
				}
			}
		}

		/**
		 * To send trail messages
		 * 
		 * @param originalRequestor
		 * @param trail
		 */
		public void sendTrail(String originalRequestor, String trail) {
			Socket socket;
			try {
				socket = new Socket(originalRequestor, 40000);
				OutputStream outToServer = socket.getOutputStream();
				DataOutputStream out = new DataOutputStream(outToServer);
				out.writeUTF("Trail: " + trail);
				socket.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		/**
		 * To send file not found messages
		 * 
		 * @param originalRequestor
		 */
		public void sendFileNotFound(String originalRequestor) {
			Socket socket;
			try {
				socket = new Socket(originalRequestor, 40000);
				OutputStream outToServer = socket.getOutputStream();
				DataOutputStream out = new DataOutputStream(outToServer);
				out.writeUTF("File not found!!!");
				socket.close();
			} catch (IOException e) {
				e.printStackTrace();
			}

		}

		/**
		 * To replicate file
		 * 
		 * @param fileName
		 * @param level
		 * @param myIndex
		 * @param indexAtLevel
		 */
		public void replicateFile(String fileName, String level,
				String myIndex, String indexAtLevel) {
			Socket socket;
			try {
				if (level.equals("0")) {
					int child1 = getHash(fileName, 1, 0);
					int child2 = getHash(fileName, 1, 1);
					socket = new Socket(hashTable[child1][1], 40000);
					OutputStream outToServer1 = socket.getOutputStream();
					DataOutputStream out1 = new DataOutputStream(outToServer1);
					out1.writeUTF("Replicate file: " + fileName);

					socket.close();
					socket = new Socket(hashTable[child2][1], 40000);
					OutputStream outToServer2 = socket.getOutputStream();
					DataOutputStream out2 = new DataOutputStream(outToServer2);
					out2.writeUTF("Replicate file: " + fileName);
					socket.close();
					Thread.sleep(2000);
					System.out.println("Replicating to: " + child1);
					fileServer
							.sendFile(fileName, myIndex, hashTable[child1][1]);
					System.out.println("Replicating to: " + child2);
					fileServer
							.sendFile(fileName, myIndex, hashTable[child2][1]);
				} else if (level.equals("1")) {
					if (indexAtLevel.equals("0")) {
						int child1 = getHash(fileName, 2, 0);
						int child2 = getHash(fileName, 2, 1);
						socket = new Socket(hashTable[child1][1], 40000);
						OutputStream outToServer1 = socket.getOutputStream();
						DataOutputStream out1 = new DataOutputStream(
								outToServer1);
						out1.writeUTF("Replicate file: " + fileName);
						socket.close();
						socket = new Socket(hashTable[child2][1], 40000);
						OutputStream outToServer2 = socket.getOutputStream();
						DataOutputStream out2 = new DataOutputStream(
								outToServer2);
						out2.writeUTF("Replicate file: " + fileName);
						socket.close();
						Thread.sleep(2000);
						FileServer fileServer = new FileServer();
						fileServer.sendFile(fileName, myIndex,
								hashTable[child1][1]);
						fileServer.sendFile(fileName, myIndex,
								hashTable[child2][1]);
					} else {
						int child1 = getHash(fileName, 2, 2);
						int child2 = getHash(fileName, 2, 3);
						socket = new Socket(hashTable[child1][1], 40000);
						OutputStream outToServer1 = socket.getOutputStream();
						DataOutputStream out1 = new DataOutputStream(
								outToServer1);
						out1.writeUTF("Replicate file: " + fileName);
						System.out.println("Replicating: " + fileName);
						socket.close();
						socket = new Socket(hashTable[child2][1], 40000);
						OutputStream outToServer2 = socket.getOutputStream();
						DataOutputStream out2 = new DataOutputStream(
								outToServer2);
						out2.writeUTF("Replicate file: " + fileName);
						System.out.println("Replicating: " + fileName);
						socket.close();
						Thread.sleep(2000);
						FileServer fileServer = new FileServer();
						fileServer.sendFile(fileName, myIndex,
								hashTable[child1][1]);
						fileServer.sendFile(fileName, myIndex,
								hashTable[child2][1]);
					}
				} else {

				}
			} catch (IOException | InterruptedException e) {
				e.printStackTrace();

			}

		}

		/**
		 * To compute hash function
		 * 
		 * @param filename
		 * @param i
		 * @param j
		 * @return
		 */
		public int getHash(String filename, int i, int j) {
			return Math.abs((filename + i + j).hashCode()) % 10;
		}

		/**
		 * To perform file look up and check if file exists
		 * 
		 * @param fileName
		 * @param myIndex
		 * @return
		 */
		public boolean fileLookUp(String fileName, String myIndex) {
			int index = Integer.parseInt(myIndex);
			File file = new File(hashTable[index][0] + "/" + fileName);
			if (file.exists()) {
				System.out.println("file exists....");
				return true;
			}
			return false;
		}
	}

	/**
	 * The server handles file transfer
	 * @author Akshai Prabhu
	 *
	 */
	public class FileServer extends Thread {
		ServerSocket fileServerSocket;
		File file;

		/**
		 * Thread run method
		 */
		public void run() {
			serverListen();
		}

		/**
		 * Listen to file transfer port
		 */
		public void serverListen() {
			Socket socket;
			while (true) {
				try {
					fileServerSocket = new ServerSocket(50000);
					socket = fileServerSocket.accept();
					file = new File(InetAddress.getLocalHost().getHostName()
							.toString()
							+ "/" + currentFileName);
					byte[] byteArray = new byte[10000];
					FileOutputStream fos = new FileOutputStream(file);
					BufferedOutputStream bos = new BufferedOutputStream(fos);
					InputStream is = socket.getInputStream();

					int count = 0;

					while ((count = is.read(byteArray)) != -1) {
						bos.write(byteArray, 0, count);
					}
					bos.flush();
					bos.close();
					fos.close();
					is.close();
					socket.close();
					fileServerSocket.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		/**
		 * To send files across the servers
		 * @param fileName
		 * @param myIndex
		 * @param IP
		 */
		public void sendFile(String fileName, String myIndex, String IP) {
			Socket socket;
			int index = Integer.parseInt(myIndex);
			try {
				socket = new Socket(IP, 50000);
				file = new File(hashTable[index][0] + "/" + fileName);
				FileInputStream fis = new FileInputStream(file);
				BufferedInputStream bis = new BufferedInputStream(fis);

				OutputStream os = socket.getOutputStream();

				byte byteArray[];
				long count = 0;

				while (count != file.length()) {
					int n = 10000;
					if (file.length() - count >= n) {
						count += n;
					} else {
						n = (int) (file.length() - count);
						count = file.length();
					}
					byteArray = new byte[n];
					bis.read(byteArray, 0, n);
					os.write(byteArray);
				}
				os.flush();
				os.close();
				bis.close();
				socket.close();
			} catch (IOException e) {
				e.printStackTrace();
			}

		}
	}

	/**
	 * Main method
	 * @param args
	 */
	public static void main(String args[]) {
		Node node = new Node();
		try {
			myIP = InetAddress.getLocalHost().getHostAddress();
		} catch (UnknownHostException e1) {
			e1.printStackTrace();
		}
		Client myClient = node.new Client();
		RequestServer myRequestServer = node.new RequestServer();
		FileServer myFileServer = node.new FileServer();
		myRequestServer.start();
		myFileServer.start();
		myClient.start();
		try {
			myClient.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
