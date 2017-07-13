import robocode.RateControlRobot
import robocode.ScannedRobotEvent
import robocode.HitByBulletEvent
import robocode.HitWallEvent
import java.awt.Color
import robocode.HitRobotEvent


class VelociRobot  extends RateControlRobot {
  
  var turnCounter = 0
  
  	override def run() : Unit = {

		turnCounter = 0
		setGunRotationRate(10)
	  setBodyColor(new Color(100, 200, 50))
		setGunColor(new Color(0, 150, 50))
		setRadarColor(new Color(0, 100, 100))
		setBulletColor(new Color(255, 255, 100))
		setScanColor(new Color(255, 200, 200))
		
		
		while (true) {
			if (turnCounter % 64 == 0) {
				// Straighten out, if we were hit by a bullet and are turning
				setTurnRate(getTurn())
				// Go forward with a velocity of 4
				setVelocityRate(6)
			}
			if (turnCounter % 64 == 32) {
				// Go backwards, faster
				setVelocityRate(-6)
				setTurnRate(getTurn())
			}
			turnCounter += 1
			execute();
		}
	}
  
  override def onScannedRobot( e : ScannedRobotEvent ) : Unit = {
    var random = Math.random();
    if( turnCounter < 200) {
      if( random < 0.3 ) {
        fire(1)
      }
    } else if( turnCounter < 300 ) {
      if( random < 0.5 ) {
        fire(1)
      }
    } else if( turnCounter < 500 ) {
      if( random < 0.7 ) {
        fire(1)
      }
    } else {
      fire(1)
    }
  }

  override def onHitRobot( e : HitRobotEvent ) : Unit = {
    scan()
  }

  var switch = true;
  
  override def onHitWall( e : HitWallEvent ) : Unit = {
      setVelocityRate( getVelocityRate() / 2 )

  }
	
  def getTurn() : Double = {
    Math.random() * 16 - 8
  }
}

