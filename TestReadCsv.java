package fr.htc.sofspak;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class TestReadCsv {

	@SuppressWarnings("resource")
	public static void main(String[] args) throws FileNotFoundException {
		// TODO Auto-generated method stub

		BufferedReader reader = null;
		// FileWriter fw = null;
		// BufferedWriter bw = null;
		FileWriter fw = null;
		BufferedWriter bw = null;
		reader = new BufferedReader(new FileReader("C:\\Users\\Desktop\\product.csv"));
		String FILENAME = "C:\\Users\\Desktop\\FichierTp\\fel.txt";
		try {
			String line = null;
			fw = new FileWriter(FILENAME);
			bw = new BufferedWriter(fw);
			while ((line = reader.readLine()) != null) {
				String[] words = line.split(";");
				// String str = words[1];
				String content = words[2];

				bw.write(content);
				bw.newLine();
				System.out.println(words[2]);

			}
			bw.close();
			// System.out.println("Done");

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException x) {
			x.printStackTrace();

		} finally {

			if (reader != null) {
				try {

					reader.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
}
