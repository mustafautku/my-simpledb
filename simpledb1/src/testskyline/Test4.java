package testskyline;

import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.RenderingHints;
import java.awt.Shape;
import java.awt.geom.Line2D;

import javax.swing.*;


/*
 * a simple Graphic sample
 */
public class Test4 extends JFrame {

    public static void main(String args[]) {
    	Test4 t = new Test4();
        t.add(new JComponent() {
            public void paintComponent(Graphics g) {
                Graphics2D g2 = (Graphics2D) g;
                g2.setRenderingHint(RenderingHints.KEY_ANTIALIASING,
                                    RenderingHints.VALUE_ANTIALIAS_ON);
                Shape l = new Line2D.Double(200,(400-200),300, (400- 300));
                g2.draw(l);
                
                // several lines.
//                for (int i = 0; i < 50; i++) {
//                    double delta = i / 10.0;
//                    double y = 5 + 5*i;
//                    Shape l = new Line2D.Double(250, y, 300, y + delta);
//                    g2.draw(l);
//                }
            }
        });

        t.setDefaultCloseOperation(EXIT_ON_CLOSE);
        t.setSize(400, 400);
        t.setVisible(true);
    }
}