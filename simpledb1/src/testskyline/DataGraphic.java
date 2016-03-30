package testskyline;


import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.RenderingHints;
import java.awt.Shape;
import java.awt.geom.Line2D;
import java.util.ArrayList;

import javax.swing.JComponent;
import javax.swing.JFrame;


public class DataGraphic extends JFrame {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public DataGraphic(final ArrayList<Double> alldata, final ArrayList<Double> skylines) {
//		final GraphicPresentation display = new GraphicPresentation(alldata,
//				skylines);
//		add(display);

		add(new JComponent() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public void paintComponent(Graphics g) {
				Graphics2D g2 = (Graphics2D) g;
				g2.setRenderingHint(RenderingHints.KEY_ANTIALIASING,
						RenderingHints.VALUE_ANTIALIAS_ON);
				g2.setStroke(new BasicStroke(2));
				// Shape l = new Line2D.Double(200,(400-200),300, (400- 300));
				// g2.draw(l);
				for (int i = 0; i < alldata.size() - 1;) {
					double x = alldata.get(i) * 400;
					double y = 400 - alldata.get(i+1) * 400;
					Shape l = new Line2D.Double(x, y, x, y);
					g2.draw(l);
					i=i+2;
				}
				
				g2.setStroke(new BasicStroke(4));
				g2.setColor(Color.RED);
				for (int i = 0; i < skylines.size() - 1;) {
					double x = skylines.get(i) * 400;
					double y = 400 - skylines.get(i+1) * 400;
					Shape l = new Line2D.Double(x, y, x, y);
					g2.draw(l);
					i=i+2;
				}

			}
		});
		
		setTitle("DOUBLE POINTS");
		setVisible(true);
		setSize(500, 500);
		setLocation(100, 100);
		// setBackground(Color.RED);
		setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);

	}

}
