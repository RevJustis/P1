import scala.io.StdIn.readChar


object Utilities {
  def chooseN(n: Byte): Byte ={
    var input: Char = readChar()
    var inByte: Byte = 0
    var goodIn: Boolean = false

    n match {
      case 1 =>
        print("Sorry, but you have to choose '1'... Huh... almost feels like you have no choice at all... OK you can go now. ");
        goodIn = true;
        inByte =  1.toByte
      case 2 =>
        while (!goodIn){
          input match {
            case '1'  => goodIn = true; inByte = 1.toByte
            case '2'  => goodIn = true; inByte = 2.toByte
            case _  => print("Sorry, but you have to choose '1', or '2': "); input = readChar()
          }
        }
      case 3 =>
        while (!goodIn){
          input match {
            case '1'  => goodIn = true; inByte = 1.toByte
            case '2'  => goodIn = true; inByte = 2.toByte
            case '3'  => goodIn = true; inByte = 3.toByte
            case _  => print("Sorry, but you have to choose '1', '2', or '3': "); input = readChar()
          }
        }
      case 4 =>
        while (!goodIn){
          input match {
            case '1'  => goodIn = true; inByte = 1.toByte
            case '2'  => goodIn = true; inByte = 2.toByte
            case '3'  => goodIn = true; inByte = 3.toByte
            case '4' => goodIn = true; inByte = 4.toByte
            case _  => print("Sorry, but you have to choose '1', '2', '3', or '4': "); input = readChar()
          }
        }
      case 5 =>
        while (!goodIn){
          input match {
            case '1'  => goodIn = true; inByte = 1.toByte
            case '2'  => goodIn = true; inByte = 2.toByte
            case '3'  => goodIn = true; inByte = 3.toByte
            case '4' => goodIn = true; inByte = 4.toByte
            case '5' => goodIn = true; inByte = 5.toByte
            case _  => print("Sorry, but you have to choose '1', '2', '3', '4', or '5': "); input = readChar()
          }
        }
      case 6 =>
        while (!goodIn){
          input match {
            case '1'  => goodIn = true; inByte = 1.toByte
            case '2'  => goodIn = true; inByte = 2.toByte
            case '3'  => goodIn = true; inByte = 3.toByte
            case '4' => goodIn = true; inByte = 4.toByte
            case '5' => goodIn = true; inByte = 5.toByte
            case '6' => goodIn = true; inByte = 6.toByte
            case _  => print("Sorry, but you have to choose '1', '2', '3', '4', '5', or '6': "); input = readChar()
          }
        }
    }
    inByte
  }
}
