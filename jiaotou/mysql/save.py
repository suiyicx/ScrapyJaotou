from jiaotou.mysql.save_course import save_course
from jiaotou.mysql.save_school import save_school


save_school('jiaotou_school:items', 10000, 0, -1)
save_course('jiaotou_course:items', 100000, 0, -1)
