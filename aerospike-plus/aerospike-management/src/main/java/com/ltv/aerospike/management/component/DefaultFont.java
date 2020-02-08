package com.ltv.aerospike.management.component;

import java.awt.Color;
import java.awt.Font;
import java.awt.font.TextAttribute;
import java.util.HashMap;
import java.util.Map;

import javax.swing.JComponent;

public class DefaultFont {
    public static void setFont(JComponent component) {
        Map<TextAttribute, Object> attributes = new HashMap<>();
        attributes.put(TextAttribute.FAMILY, "Arial");
        attributes.put(TextAttribute.WEIGHT, 0.01F);
        attributes.put(TextAttribute.SIZE, 12);
        component.setFont(Font.getFont(attributes));
        component.setForeground(Color.DARK_GRAY.darker());
    }
}
