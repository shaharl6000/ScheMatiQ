import type { NavigationBlocker } from '@/hooks/useNavigationGuard';
import {
  AlertDialog,
  AlertDialogContent,
  AlertDialogHeader,
  AlertDialogTitle,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogCancel,
  AlertDialogAction,
} from '@/components/ui/alert-dialog';

interface NavigationConfirmDialogProps {
  blocker: NavigationBlocker;
  title: string;
  description: string;
}

export function NavigationConfirmDialog({ blocker, title, description }: NavigationConfirmDialogProps) {
  if (blocker.state !== 'blocked') return null;

  return (
    <AlertDialog open>
      <AlertDialogContent>
        <AlertDialogHeader>
          <AlertDialogTitle>{title}</AlertDialogTitle>
          <AlertDialogDescription>{description}</AlertDialogDescription>
        </AlertDialogHeader>
        <AlertDialogFooter>
          <AlertDialogCancel onClick={() => blocker.reset()}>Stay</AlertDialogCancel>
          <AlertDialogAction onClick={() => blocker.proceed()}>Leave</AlertDialogAction>
        </AlertDialogFooter>
      </AlertDialogContent>
    </AlertDialog>
  );
}
