import java.io.*;
import java.net.*;

import java.util.Arrays;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class Client {

	static class Server {
		public String type;
		public int limit;
		public int bootupTime;
		public float hourlyRate;
		public int coreCount;
		public int memory;
		public int disk;

		public Server(String type, int limit, int bootupTime, float hourlyRate, int coreCount, int memory, int disk) {
			this.type = type;
			this.limit = limit;
			this.bootupTime = bootupTime;
			this.hourlyRate = hourlyRate;
			this.coreCount = coreCount;
			this.memory = memory;
			this.disk = disk;
		}
	}

	static class ServerConnection {
		public Socket s;
		public BufferedReader recieveFromServer;
		public DataOutputStream sendToServer;

		public ServerConnection() throws IOException {
			// Initialise server connection to localhost, and both read and write methods
			this.s = new Socket("127.0.0.1", 50000);
			this.recieveFromServer = new BufferedReader(new InputStreamReader(s.getInputStream()));
			this.sendToServer = new DataOutputStream(s.getOutputStream());
		}

		public void send(String msg) throws IOException {
			sendToServer.write((msg + "\n").getBytes());
		}

		public String read() throws IOException {
			StringBuilder result = new StringBuilder();
			while(result.length() < 1) {
				while(this.recieveFromServer.ready()) {
					result.append(this.recieveFromServer.readLine());
				}
			}
			return result.toString();
		}

		public void flush() throws IOException {
			sendToServer.flush();
		}

		public void close() throws IOException {
			this.send("QUIT");
			sendToServer.close();
			recieveFromServer.close();
			s.close();
		}
	}

	static class JOBN {
		public int submitTime;
		public int jobID;
		public int estRuntime;
		public int core;
		public int memory;
		public int disk;

		public JOBN(String msg) {
			String[] brokenString = msg.split(" ");
			this.submitTime = Integer.parseInt(brokenString[1]);
			this.jobID = Integer.parseInt(brokenString[2]);
			this.estRuntime = Integer.parseInt(brokenString[3]);
			this.core = Integer.parseInt(brokenString[4]);
			this.memory = Integer.parseInt(brokenString[5]);
			this.disk = Integer.parseInt(brokenString[6]);
		}
	}

	public static void sortServerList(Server[] serverList) {
		// Sort first by server core count, if equal, sort by name
		Arrays.sort(serverList, (a, b) -> {
			String nameA = a.type.replaceAll("\\d", "");
			String nameB = b.type.replaceAll("\\d", "");

			if (nameA.equals(nameB) && a.coreCount == b.coreCount) {
				return (a.type.compareTo(b.type) * -1);
			} else {
				return Integer.compare(a.coreCount, b.coreCount);
			}
		});
	}

	public static Server[] importXML(String fileLocation) throws ParserConfigurationException, SAXException, IOException {

		Server[] returning = new Server[]{};

		File dssystemxml = new File(fileLocation);

		DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder docBuilder = docBuilderFactory.newDocumentBuilder();
		Document doc = docBuilder.parse(dssystemxml);

		doc.getDocumentElement().normalize();

		NodeList servers = doc.getElementsByTagName("server");

		Server[] serverList = new Server[servers.getLength()];
		returning = new Server[servers.getLength()];

		// Add each server in the XML server to a class instance, to be added to an array
		for(int i = 0; i < servers.getLength(); i++) {
			Element server = (Element) servers.item(i);

			String t = server.getAttribute("type");
			int l = Integer.parseInt(server.getAttribute("limit"));
			int b = Integer.parseInt(server.getAttribute("bootupTime"));
			float hr = Float.parseFloat(server.getAttribute("hourlyRate"));
			int c = Integer.parseInt(server.getAttribute("coreCount"));
			int m = Integer.parseInt(server.getAttribute("memory"));
			int d = Integer.parseInt(server.getAttribute("disk"));

			serverList[i] = new Server(t, l, b, hr, c, m, d);
		}

		returning = serverList.clone();

		return returning;
	}

	public static void allToLargest(ServerConnection SC, int jobID, Server[] serverList) throws IOException {
		SC.send("SCHD " + jobID + " " + serverList[serverList.length - 1].type + " 0");
	}
	
	public static void main(String[] args) {  
		try {
			ServerConnection SC = new ServerConnection();

			// Initial server message transfer
			SC.send("HELO");
			SC.read();

			SC.send("AUTH " + System.getProperty("user.name"));
			SC.read();

			Server[] serverList = importXML("./ds-system.xml");
			
			sortServerList(serverList);

			// Recieve jobs and send them allToLargest server until no more are recieved
			runner: while (true) {
				SC.send("REDY");
				String serverResponse = SC.read();
				String[] serverResponseSplit = serverResponse.split(" ");

				if (serverResponseSplit[0].equals("JOBN")) {
					JOBN jobn1 = new JOBN(serverResponse);
					allToLargest(SC, jobn1.jobID, serverList);
					SC.read();
					}

				if (serverResponseSplit[0].equals("NONE")) {
					break runner;
				}
			}

			SC.flush();
			SC.close(); 
		} catch (Exception e) {
			System.out.println(e);
		}
	}  
}