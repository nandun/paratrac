class Hue
  def Hue::to_rgb(h)
    h = h % (2 * Math::PI)
    c = (h / (Math::PI / 3)).to_i
    p = ((h % (Math::PI / 3)) / (Math::PI / 3) * 255).to_i
    case c
    when 0
      [255, p, 0]
    when 1
      [255 - p, 255, 0]
    when 2
      [0, 255, p]
    when 3
      [0, 255 - p, 255]
    when 4
      [p, 0, 255]
    when 5
      [255, 0, 255 - p]
    end 
  end
end
