/*
 * DiSNI: Direct Storage and Networking Interface
 *
 * Author: Patrick Stuedi <stu@zurich.ibm.com>
 *
 * Copyright (C) 2016-2018, IBM Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.ibm.disni.examples;

import com.ibm.disni.CmdLineCommon;
import com.ibm.disni.RdmaActiveEndpoint;
import com.ibm.disni.RdmaActiveEndpointGroup;
import com.ibm.disni.RdmaEndpointFactory;
import com.ibm.disni.RdmaServerEndpoint;
import com.ibm.disni.verbs.*;
import org.apache.commons.cli.ParseException;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.concurrent.ArrayBlockingQueue;

public class SendRecvServer implements RdmaEndpointFactory<CustomServerEndpoint> {
	RdmaActiveEndpointGroup<CustomServerEndpoint> endpointGroup;
	private String host;
	private int port;

	public CustomServerEndpoint createEndpoint(RdmaCmId idPriv, boolean serverSide) throws IOException {
		return new CustomServerEndpoint(endpointGroup, idPriv, serverSide, 100);
	}

	public void run() throws Exception {
		//create a EndpointGroup. The RdmaActiveEndpointGroup contains CQ processing and delivers CQ event to the endpoint.dispatchCqEvent() method.
		endpointGroup = new RdmaActiveEndpointGroup<CustomServerEndpoint>(1000, false, 128, 4, 128);
		endpointGroup.init(this);
		//create a server endpoint
		RdmaServerEndpoint<CustomServerEndpoint> serverEndpoint = endpointGroup.createServerEndpoint();

		//we can call bind on a server endpoint, just like we do with sockets
		InetAddress ipAddress = InetAddress.getByName(host);
		InetSocketAddress address = new InetSocketAddress(ipAddress, port);				
		serverEndpoint.bind(address, 10);
		System.out.println("SimpleServer::servers bound to address " + address.toString());

		//we can accept new connections
		CustomServerEndpoint clientEndpoint = serverEndpoint.accept();
		//we have previously passed our own endpoint factory to the group, therefore new endpoints will be of type CustomServerEndpoint
		System.out.println("SimpleServer::client connection accepted");

		//in our custom endpoints we have prepared (memory registration and work request creation) some memory buffers beforehand.
		ByteBuffer sendBuf = clientEndpoint.getSendBuf();
		sendBuf.asCharBuffer().put("Hello from the server");
		sendBuf.clear();

		//in our custom endpoints we make sure CQ events get stored in a queue, we now query that queue for new CQ events.
		//in this case a new CQ event means we have received data, i.e., a message from the client.
		clientEndpoint.getWcEvents().take();
		System.out.println("SimpleServer::message received");
		ByteBuffer recvBuf = clientEndpoint.getRecvBuf();
		recvBuf.clear();
		System.out.println("Message from the client: " + recvBuf.asCharBuffer().toString());
		//let's respond with a message
		clientEndpoint.postSend(clientEndpoint.getWrList_send()).execute().free();
		//when receiving the CQ event we know the message has been sent
		clientEndpoint.getWcEvents().take();
		System.out.println("SimpleServer::message sent");

		//close everything
		clientEndpoint.close();
		System.out.println("client endpoint closed");
		serverEndpoint.close();
		System.out.println("server endpoint closed");
		endpointGroup.close();
		System.out.println("group closed");
//		System.exit(0);
	}

	public void launch(String[] args) throws Exception {
		CmdLineCommon cmdLine = new CmdLineCommon("SendRecvServer");

		try {
			cmdLine.parse(args);
		} catch (ParseException e) {
			cmdLine.printHelp();
			System.exit(-1);
		}
		host = cmdLine.getIp();
		port = cmdLine.getPort();

		this.run();
	}

	public static void main(String[] args) throws Exception {
		SendRecvServer simpleServer = new SendRecvServer();
		simpleServer.launch(args);
	}



}

