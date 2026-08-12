use ratatui::{DefaultTerminal, Frame};
use std::io;

fn main() -> std::io::Result<()> {
    ratatui::run(|terminal| App::default().run(terminal))?;
    Ok(())
}

#[derive(Debug, Default)]
pub struct App {
    counter: u8,
    exit: bool,
}

impl App {
    /// runs the application's main loop until the user quits
    pub fn run(&mut self, terminal: &mut DefaultTerminal) -> io::Result<()> {
        while !self.exit {
            terminal.draw(|frame| self.draw(frame))?;
            self.handle_events()?;
        }
        Ok(())
    }

    fn draw(&self, frame: &mut Frame) {
        frame.render_widget("hello world", frame.area());
    }

    fn handle_events(&mut self) -> io::Result<()> {
        let event = crossterm::event::read()?;
        let crossterm::event::Event::Key(key_event) = event else {
            return Ok(());
        };
        match key_event.code {
            crossterm::event::KeyCode::Esc => self.exit = true,

            _ => {}
        }
        Ok(())
    }
}
