"""Render the project's original geometric icon; build-time Pillow only.

No external artwork, user image, font or network is read. Not imported by HA.
Geometry corresponds to the adjacent SVG and is rendered with supersampling.
"""
from pathlib import Path

from PIL import Image, ImageDraw


def render(size=256):
    scale = 4 * size / 512
    canvas = Image.new("RGBA", (size * 4, size * 4), (0, 0, 0, 0))
    draw = ImageDraw.Draw(canvas)
    def box(values):
        return tuple(round(value * scale) for value in values)
    def round_rect(values, radius, fill=None, outline=None, width=1):
        draw.rounded_rectangle(box(values), radius=round(radius * scale), fill=fill,
                               outline=outline, width=round(width * scale))
    def circle(center, radius, fill=None, outline=None, width=1):
        x, y = center
        draw.ellipse(box((x-radius, y-radius, x+radius, y+radius)), fill=fill,
                     outline=outline, width=round(width * scale))
    def line(points, color, width):
        draw.line([box(point) for point in points], fill=color,
                  width=round(width * scale), joint="curve")
        for point in points:
            circle(point, width / 2, fill=color)
    round_rect((16,16,496,496), 104, fill="#10263a")
    # SVG outlines are centered; Pillow strokes sit inside their bounds.
    round_rect((65,127,357,381), 53, outline="#f4f8fc", width=22)
    circle((199,254), 78, outline="#54dccf", width=26)
    circle((302,174), 8, fill="#54dccf")
    line([(286,254),(425,254)], "#54dccf", 26)
    line([(387,216),(425,254),(387,292)], "#54dccf", 26)
    line([(146,416),(358,416)], "#8fb3c2", 10)
    for x in (124,230,336):
        round_rect((x,394,x+44,438), 12, fill="#54dccf")
    return canvas.resize((size,size), Image.Resampling.LANCZOS)


if __name__ == "__main__":
    icon = render()
    icon.save(Path(__file__).with_name("icon.png"), format="PNG", optimize=True)
    print("icon=original, size=256x256, format=PNG, mode=RGBA")
