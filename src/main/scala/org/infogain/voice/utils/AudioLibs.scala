package org.infogain.voice.utils

import edu.cmu.sphinx.api.{Configuration, StreamSpeechRecognizer}

object AudioConf {
  val configuration: Configuration = new Configuration
  configuration.setAcousticModelPath("resource:/edu/cmu/sphinx/models/en-us/en-us")
  configuration.setDictionaryPath("resource:/edu/cmu/sphinx/models/en-us/cmudict-en-us.dict")
  configuration.setLanguageModelPath("resource:/edu/cmu/sphinx/models/en-us/en-us.lm.bin")
  configuration.setSampleRate(22050)
}

class AudioLibs {
  val recognizer: StreamSpeechRecognizer = new StreamSpeechRecognizer(AudioConf.configuration)
}