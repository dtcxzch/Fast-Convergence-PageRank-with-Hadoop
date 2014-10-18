import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.util.ArrayList;

import org.apache.commons.lang.StringUtils;

import simplePageRank.SimplePageRank;
//import randomBlockedMapReduce.RandomBlockedPageRankDriver;
//import blockedMapReduce.BlockedPageRank;

public class DataProcessor {

	private final static String OUTPUT_LOCATION = "output/";

	/**
	 * @param args
	 *            This class used to process the files
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		// compute the rejeced values based on my netid
		computeRejectValues();
		// build the final preprocessed file
		buildFinalPreprocessFile();
	}

	public static void buildFinalPreprocessFile() throws IOException {
		System.out.println("Building the preprocessed file");
		// Open the file
		FileInputStream fs = new FileInputStream(OUTPUT_LOCATION
				+ "edges_remained.txt");

		// Get the object of DataInputStream
		DataInputStream in = new DataInputStream(fs);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		// Get the object of output
		File f = new File(OUTPUT_LOCATION + "zx78.txt");
		FileWriter fw = new FileWriter(f);
		BufferedWriter bw = new BufferedWriter(fw);

		ArrayList<Integer> dstNodes = new ArrayList<Integer>();
		int prevSrcNode = -1;
		float pageRank = (float) 1.0 / SimplePageRank.NUMBER_OF_NODES;

		String s = br.readLine();
		// Read File Line By Line
		while (s != null) {
			String[] elems = s.trim().split("\\s+");
			int srcNode = Integer.parseInt(elems[0]);
			if (srcNode == prevSrcNode || prevSrcNode == -1) {
				if (prevSrcNode == -1) {
					int prevNode = 0;
					while (prevNode < srcNode) {
						String output = prevNode + " "
								+ String.valueOf(pageRank) + " 0 \n";
						bw.write(output);
						prevNode++;
					}
				}
				int dstNode = Integer.parseInt(elems[1]);
				if (!dstNodes.contains(dstNode))
					dstNodes.add(dstNode);
			} else {
				int srcDegree = dstNodes.size();
				// BigDecimal pageRank = new BigDecimal(1.0).divide(new
				// BigDecimal(srcDegree));
				String output = String.valueOf(prevSrcNode) + " "
						+ String.valueOf(pageRank) + " "
						+ String.valueOf(srcDegree) + " "
						+ StringUtils.join(dstNodes, ',') + "\n";
				bw.write(output);
				int prevNode = prevSrcNode + 1;
				while (prevNode < srcNode) {
					output = prevNode + " " + String.valueOf(pageRank)
							+ " 0 \n";
					bw.write(output);
					prevNode++;
				}
				int dstNode = Integer.parseInt(elems[1]);
				dstNodes = new ArrayList<Integer>();
				if (!dstNodes.contains(dstNode))
					dstNodes.add(dstNode);
			}
			prevSrcNode = srcNode;
			s = br.readLine();
		}
		// The last line of the
		int srcDegree = dstNodes.size();
		String output = String.valueOf(prevSrcNode) + " " + pageRank + " "
				+ String.valueOf(srcDegree) + " "
				+ StringUtils.join(dstNodes, ',') + "\n";
		bw.write(output);
		int prevNode = prevSrcNode + 1;
		while (prevNode < SimplePageRank.NUMBER_OF_NODES) {
			output = prevNode + " " + String.valueOf(pageRank) + " 0 \n";
			bw.write(output);
			prevNode++;
		}
		in.close();
		bw.close();
		System.out.println("Preprocessed file is saved in " + OUTPUT_LOCATION
				+ "zx78.txt");
	}

	/**
	 * computeRejectValues Function to compute the rejected values based on the
	 * netid
	 * 
	 * @throws IOException
	 */
	public static void computeRejectValues() throws IOException {
		System.out.println("Building the edge list based on NetID: zx78");
		// compute filter parameters for netid zx78
		// Specifically, take the digits of your netid and write them in
		// reverse order preceded by a decimal point.
		double fromNetID = 0.87;
		double rejectMin = 0.99 * fromNetID;
		double rejectLimit = rejectMin + 0.01;
		System.out.println("rejectMin is " + String.format("%.4f", rejectMin));
		System.out.println("rejectLimit is "
				+ String.format("%.4f", rejectLimit));

		// Open the file
		FileInputStream fs = new FileInputStream("edges.txt");

		// Get the object of DataInputStream
		DataInputStream in = new DataInputStream(fs);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));

		String s = br.readLine();

		int totCount = 0;
		int rejCount = 0;
		File f1 = new File(OUTPUT_LOCATION + "edges_remained.txt");
		File f2 = new File(OUTPUT_LOCATION + "edges_rejected.txt");
		FileWriter fw1 = new FileWriter(f1);
		FileWriter fw2 = new FileWriter(f2);
		BufferedWriter bw1 = new BufferedWriter(fw1);
		BufferedWriter bw2 = new BufferedWriter(fw2);

		System.out.println("Loading file edges.txt");
		// Read File Line By Line
		while (s != null) {
			String[] elems = s.trim().split("\\s+");
			BigDecimal edgeValue = new BigDecimal(elems[2]);

			if (edgeValue.floatValue() >= rejectMin
					&& edgeValue.floatValue() < rejectLimit) {
				rejCount += 1;
				bw2.write(s + "\n");
			} else {
				bw1.write(s + "\n");
			}

			s = br.readLine();
			totCount += 1;
		}

		// Close the input and output stream
		in.close();
		bw1.close();
		bw2.close();
		System.out.println("Rejected edges are saved in " + OUTPUT_LOCATION
				+ "edges_rejected.txt");
		System.out.println("Remained edges are saved in " + OUTPUT_LOCATION
				+ "edges_remained.txt");
		System.out.println("Totally edges: " + totCount);
		System.out.println("Rejected edges: " + rejCount);
		System.out.println("Remained edges: " + (totCount - rejCount));
		float rejRate = (float) rejCount / totCount;
		float remRate = (float) (totCount - rejCount) / totCount;
		System.out.println("Rejected rate: " + String.format("%5.2f", rejRate));
		System.out.println("Remained rate: " + String.format("%5.2f", remRate));
	}

}
